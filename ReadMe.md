# Starter Template

TypeScript starter template.

## Spark Connect server

`src/spark_connect.rs` embeds [Sail](https://github.com/lakehq/sail)'s Spark
Connect server in the Node.js process. It is the counterpart of
`pysail.spark.SparkConnectServer`, so the Sail Python sample ports over as-is:

```ts
import { SparkConnectServer, connectSparkSession } from "./src/spark/index.ts";

const server = new SparkConnectServer();
const { ip, port } = await server.start();

const spark = await connectSparkSession(`sc://${ip}:${port}`);
await spark.sql("SELECT 1 + 1").show();
await spark.stop();
await server.stop();
```

`src/spark/example.ts` is that sample converted end to end (run it with
`npm run example:spark`).

Sail is not published on crates.io and pulls in DataFusion, Arrow, tonic and
pyo3, so it sits behind a Cargo feature and is off by default:

```bash
npm run build:napi -- --features spark-connect
```

Configuration is Sail's own: `AppConfig::load()` merges `SAIL_*` environment
variables over the defaults embedded in the binary, where `__` separates
nesting levels (`SAIL_SPARK__SESSION_TIMEOUT_SECS` is
`spark.session_timeout_secs`). The `sessionTimeoutSecs` option overrides the
same field after loading.

The server needs a Spark Connect client on the JavaScript side; there is no
first-party one, so install the community client:

```bash
npm install @spark-connect-js/node
```

Known gaps in the draft:

- Telemetry is not initialized. Sail only exposes `TelemetryGuard` from
  `sail-telemetry`, which is a dev-dependency of `sail-spark-connect`.
- `pyo3` (0.29, only the `serde` feature) is a non-optional dependency of
  `sail-spark-connect`, so a Python interpreter must be discoverable at build
  time (`PYO3_PYTHON` if it is not on `PATH`). Nothing links `libpython` on
  macOS, so the addon loads there as is; on Linux and Windows pyo3 links it by
  default. Opt out with

  ```toml
  pyo3 = { version = "0.29", features = ["extension-module"] }
  ```

  in this crate, which switches the link off through feature unification at the
  cost of Python UDFs (`sail-python-udf`).
- TLS is expected to be terminated by a gateway in front of the plaintext gRPC
  listener, same as upstream.
- Sail `v0.7.2` pins DataFusion `55.1.0`, while the bindings in `src/sql.rs`
  use `55.2.0`, so enabling the feature compiles both copies into the addon.
  Splitting the Spark Connect bindings into their own crate avoids that.

### PySpark shell (`sail spark shell`)

`src/cli.rs` is the Node equivalent of Sail's `sail spark shell`: a Spark Connect
server in this process plus an embedded PySpark REPL that connects back to it
over loopback. The Python side is `src/python/spark_shell.py`, compiled into the
addon; it mirrors upstream's `crates/sail-cli/src/python/spark_shell.py`
(Apache-2.0).

```bash
npm run build:napi -- --features spark-shell
PYO3_PYTHON=/path/to/python-with-pyspark npm run spark:shell
```

```
Welcome to
  ____              __
 / __/___  ___ ___/ /__
_\\ \/ _ \/ _ `/ ___/  '_/
/__ / .__/\_,_/ /_/\__\   version 4.0.1
   /_/

Client connected to the Sail Spark Connect server at localhost:53411
SparkSession available as 'spark'.
>>> spark.sql("SELECT 1 + 1").show()
```

Two entry points, because CPython only runs signal handlers on the process's
main thread:

- `runPySparkShell()` blocks Node's main thread until the user leaves the REPL.
  This is what `npm run spark:shell` runs, and it behaves like the Rust CLI,
  Ctrl-C included. The server runs on its own tokio runtime, so queries still
  execute while the event loop is blocked.
- `SparkShell` runs the REPL on a worker thread so a long-running process keeps
  its event loop. Call `preparePython()` first (it initializes the interpreter
  on the main thread), then `await shell.start()`. `stop()` shuts the server
  down; the REPL itself ends when the user leaves it.

Requirements and caveats:

- The `spark-shell` feature embeds CPython (`pyo3` 0.29), so the addon links
  `libpython` and the interpreter needs PySpark installed. Set `PYO3_PYTHON` if
  it is not the one on `PATH`.
- `SAIL_*` environment variables apply as before, e.g.
  `SAIL_SPARK__SESSION_TIMEOUT_SECS=3600` to keep a session alive while you work
  at the prompt.
- The REPL owns stdin/stdout: only one shell per process, and it has to run in a
  terminal.

## Development

- Configure local hooks:

```bash
npm run prepare
```

- Install dependencies:

```bash
vp install
```

- Run the unit tests:

```bash
vp test
```

- Run the locally:

```bash
npm run dev
```

- Build the library:

```bash
npm run build
```

- Code formatting:

```bash
npm run fmt
```

- Linting:

```bash
npm run lint
```

- Code check:

```bash
npm run check
```
