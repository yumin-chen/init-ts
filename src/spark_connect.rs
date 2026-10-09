use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard};

use napi::{Error, Result, Status};
use napi_derive::napi;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeManager;
use sail_spark_connect::entrypoint::serve;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Default bind address: loopback only, mirroring `pysail`'s default.
const DEFAULT_IP: &str = "127.0.0.1";

/// Turn any Sail/tokio error into a `napi::Error`.
fn to_napi_err<E: std::fmt::Display>(err: E) -> Error {
  Error::new(Status::GenericFailure, err.to_string())
}

/// Resolve an invalid-argument error.
fn invalid_arg(message: impl Into<String>) -> Error {
  Error::new(Status::InvalidArg, message.into())
}

/// Parse the `ip` option, accepting the literal name `localhost`.
fn parse_ip(value: Option<&str>) -> Result<IpAddr> {
  match value {
    None => Ok(IpAddr::V4(Ipv4Addr::LOCALHOST)),
    Some("localhost") => Ok(IpAddr::V4(Ipv4Addr::LOCALHOST)),
    Some(value) => value
      .parse::<IpAddr>()
      .map_err(|e| invalid_arg(format!("invalid ip address {value:?}: {e}"))),
  }
}

/// The address a running server is bound to, the JS form of pysail's
/// `listening_address` tuple.
#[napi(object)]
pub struct ListeningAddress {
  /// The IP address the server is bound to.
  pub ip: String,
  /// The TCP port the server is bound to. `0` on input is resolved by the OS
  /// to a free ephemeral port.
  pub port: u16,
}

impl From<SocketAddr> for ListeningAddress {
  fn from(address: SocketAddr) -> Self {
    Self {
      ip: address.ip().to_string(),
      port: address.port(),
    }
  }
}

/// Options accepted by [`SparkConnectServer`], mirroring `SparkConnectServer(ip, port)`.
#[derive(Default)]
#[napi(object)]
pub struct SparkConnectServerOptions {
  /// IP address to bind. Defaults to `127.0.0.1` (loopback only).
  pub ip: Option<String>,
  /// Port to bind. Defaults to `0`, letting the OS pick an ephemeral port.
  pub port: Option<u16>,
  /// Overrides `spark.session_timeout_secs`, the same value that
  /// `SAIL_SPARK__SESSION_TIMEOUT_SECS` sets. Handy to keep long-lived
  /// notebook-style sessions from expiring while idle.
  pub session_timeout_secs: Option<i64>,
}

/// The server task: `serve()` resolves once the shutdown signal fires, or with
/// the error message if the gRPC server fails.
type ServerTask = tokio::task::JoinHandle<Result<(), String>>;

/// Mutable state guarded by a plain mutex.
///
/// The lock is never held across an `.await`: every critical section is a few
/// field reads or writes, and the getters are synchronous (a `tokio::sync::Mutex`
/// could not be locked from them).
struct Inner {
  /// Loaded once, in the constructor. Everything that needs configuration
  /// (`serve`, the runtime manager) shares this `Arc`.
  config: Arc<AppConfig>,
  /// Kept alive for as long as this object exists: it owns the two tokio
  /// runtimes that execute the server. `RuntimeManager` must not be dropped
  /// from inside an async context (tokio panics on that), so it is released
  /// when the JS object is collected, never in [`SparkConnectServer::stop`].
  _runtime: Option<RuntimeManager>,
  /// `Some` exactly while the server is listening.
  address: Option<SocketAddr>,
  /// Fires the graceful-shutdown signal passed to `serve()`.
  shutdown: Option<oneshot::Sender<()>>,
  /// The spawned server task, awaited by `stop()`.
  task: Option<ServerTask>,
}

/// Sail's Spark Connect server, embedded in the Node.js process.
///
/// ```ts
/// const server = new SparkConnectServer();
/// const { ip, port } = await server.start();
/// const spark = await connectSparkSession(`sc://${ip}:${port}`);
/// ```
#[napi]
pub struct SparkConnectServer {
  options: SparkConnectServerOptions,
  state: Arc<Mutex<Inner>>,
}

impl SparkConnectServer {
  /// Lock the state, recovering from poisoning: a panicking server task must
  /// not turn every later call into a panic.
  fn lock(&self) -> MutexGuard<'_, Inner> {
    self
      .state
      .lock()
      .unwrap_or_else(|poisoned| poisoned.into_inner())
  }
}

#[napi]
impl SparkConnectServer {
  /// Create a server. Nothing is bound until [`SparkConnectServer::start`] is
  /// called.
  #[napi(constructor)]
  pub fn new(options: Option<SparkConnectServerOptions>) -> Result<Self> {
    let options = options.unwrap_or_default();

    // Validate eagerly so a typo in `ip` is not reported from `start()`.
    parse_ip(options.ip.as_deref())?;
    if let Some(timeout) = options.session_timeout_secs {
      if timeout < 0 {
        return Err(invalid_arg("sessionTimeoutSecs must not be negative"));
      }
    }

    // Same loading path as the Sail CLI: embedded defaults, then `SAIL_*`
    // environment variables (`SAIL_SPARK__SESSION_TIMEOUT_SECS` and friends).
    let mut config = AppConfig::load().map_err(to_napi_err)?;
    if let Some(timeout) = options.session_timeout_secs {
      config.spark.session_timeout_secs = timeout as u64;
    }

    Ok(Self {
      options,
      state: Arc::new(Mutex::new(Inner {
        config: Arc::new(config),
        _runtime: None,
        address: None,
        shutdown: None,
        task: None,
      })),
    })
  }

  /// Bind the address and start serving in the background. Resolves with the
  /// address actually bound, which is how you learn the port when `port` was
  /// left at `0`.
  #[napi]
  pub async fn start(&self) -> Result<ListeningAddress> {
    let ip = parse_ip(self.options.ip.as_deref())?;
    let port = self.options.port.unwrap_or_default();

    if self.lock().address.is_some() {
      return Err(Error::new(
        Status::GenericFailure,
        "the Spark Connect server is already running",
      ));
    }

    let config = Arc::clone(&self.lock().config);
    let runtime = RuntimeManager::try_new(&config.runtime).map_err(to_napi_err)?;
    let handle = runtime.handle();

    // Bind on Sail's own runtime: the listener registers its I/O driver with
    // the runtime that creates it, and `serve()` polls it from the primary
    // runtime. Binding here and moving the listener out of this task keeps
    // that association intact without a `block_on` (which would panic inside
    // the async NAPI call).
    let (bound_tx, bound_rx) = oneshot::channel();
    handle.primary().spawn(async move {
      let bound = TcpListener::bind(SocketAddr::new(ip, port))
        .await
        .and_then(|listener| listener.local_addr().map(|addr| (addr, listener)));
      let _ = bound_tx.send(bound);
    });
    let bound = bound_rx.await.map_err(|e| {
      Error::new(
        Status::GenericFailure,
        format!("the Sail runtime stopped before the server could bind: {e}"),
      )
    })?;
    let (address, listener) =
      bound.map_err(|e| Error::new(Status::GenericFailure, format!("failed to bind {ip}:{port}: {e}")))?;

    let (shutdown, signal) = oneshot::channel::<()>();
    let serve_config = Arc::clone(&config);
    let serve_handle = handle.clone();
    let task: ServerTask = handle.primary().spawn(async move {
      // `serve` wants a `Future<Output = ()>`, and a `Receiver` resolves to
      // `Result<(), RecvError>`, so swallow the receive error.
      let signal = async move {
        let _ = signal.await;
      };
      // `serve` returns `Box<dyn Error>`, which is not `Send`: map it to a
      // string before it crosses the task boundary.
      serve(listener, signal, serve_config, serve_handle)
        .await
        .map_err(|e| e.to_string())
    });

    let mut inner = self.lock();
    inner._runtime = Some(runtime);
    inner.address = Some(address);
    inner.shutdown = Some(shutdown);
    inner.task = Some(task);

    Ok(ListeningAddress::from(address))
  }

  /// Signal graceful shutdown and wait for the server task to finish. Safe to
  /// call when the server was never started.
  #[napi]
  pub async fn stop(&self) -> Result<()> {
    let (shutdown, task) = {
      let mut inner = self.lock();
      (inner.shutdown.take(), inner.task.take())
    };

    if let Some(shutdown) = shutdown {
      // An error here only means the server task is already gone.
      let _ = shutdown.send(());
    }
    if let Some(task) = task {
      match task.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
          return Err(Error::new(
            Status::GenericFailure,
            format!("the Spark Connect server failed: {e}"),
          ));
        }
        Err(e) => {
          return Err(Error::new(
            Status::GenericFailure,
            format!("the Spark Connect server task panicked: {e}"),
          ));
        }
      }
    }

    self.lock().address = None;
    Ok(())
  }

  /// The bound address while the server is running, `null` otherwise.
  #[napi(getter)]
  pub fn listening_address(&self) -> Option<ListeningAddress> {
    self.lock().address.map(ListeningAddress::from)
  }

  /// Whether the server is currently listening.
  #[napi(getter)]
  pub fn running(&self) -> bool {
    self.lock().address.is_some()
  }

  /// The `sc://host:port` URL a Spark Connect client should connect to.
  #[napi]
  pub fn connection_url(&self) -> Result<String> {
    let address = self.lock().address.ok_or_else(|| {
      Error::new(
        Status::GenericFailure,
        "the Spark Connect server is not running",
      )
    })?;
    Ok(format!(
      "sc://{}:{}",
      if address.is_ipv6() {
        format!("[{}]", address.ip())
      } else {
        address.ip().to_string()
      },
      address.port()
    ))
  }
}

impl Drop for SparkConnectServer {
  /// Runs on the JS thread when the object is collected: ask the server to
  /// stop and let the task finish on its own. Waiting is not an option here.
  fn drop(&mut self) {
    let mut inner = self.lock();
    if let Some(shutdown) = inner.shutdown.take() {
      let _ = shutdown.send(());
    }
    inner.task = None;
  }
}

// Not exposed to JS:
//
//   - `sail_telemetry::TelemetryGuard` (the CLI initializes tracing before
//     serving; Sail ships it as a dev-dependency only, so Node hosts get the
//     server without telemetry).
//   - TLS: Sail expects a gateway to terminate it in front of the plaintext
//     gRPC listener.
//   - `serve_with_session_factory`, which lets an embedder supply its own
//     session factory instead of Sail's DataFusion-backed default.