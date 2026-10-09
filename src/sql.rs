use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::error::DataFusionError;
use datafusion::prelude::{
  AvroReadOptions, CsvReadOptions, ParquetReadOptions, SessionContext as DFSessionContext,
};
use napi::Status;
use napi_derive::napi;

/// Convert a `DataFusionError` into a `napi::Error`.
pub(crate) fn to_napi_err(err: DataFusionError) -> napi::Error {
  napi::Error::new(Status::GenericFailure, err.to_string())
}

/// Minimal JSON string escaper so `to_json` needs no `serde_json` dependency.
fn json_escape(s: &str) -> String {
  let mut out = String::with_capacity(s.len() + 2);
  for c in s.chars() {
    match c {
      '"' => out.push_str("\\\""),
      '\\' => out.push_str("\\\\"),
      '\n' => out.push_str("\\n"),
      '\r' => out.push_str("\\r"),
      '\t' => out.push_str("\\t"),
      _ => out.push(c),
    }
  }
  out
}

/// Result of running a SQL statement.
///
/// Cells are materialized as `string | null` so the data is trivially
/// consumable from JavaScript. For a typed, columnar representation you would
/// inspect the Arrow schema instead (out of scope for this draft).
#[napi(object)]
pub struct QueryResult {
  /// Column names, in order.
  pub columns: Vec<String>,
  /// Rows, each a parallel array of cells matching `columns`.
  pub rows: Vec<Vec<Option<String>>>,
  /// Number of rows returned.
  pub row_count: i64,
}

impl QueryResult {
  /// Build rows from Arrow `RecordBatch`es by stringifying each cell.
  pub(crate) fn from_batches(batches: Vec<RecordBatch>) -> Self {
    let columns: Vec<String> = batches
      .first()
      .map(|b| b.schema().fields().iter().map(|f| f.name().clone()).collect())
      .unwrap_or_default();

    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    for batch in &batches {
      let cols: Vec<&dyn Array> = (0..batch.num_columns())
        .map(|i| batch.column(i).as_ref())
        .collect();
      for r in 0..batch.num_rows() {
        let row: Vec<Option<String>> = cols
          .iter()
          .map(|c| {
            if c.is_null(r) {
              None
            } else {
              array_value_to_string(*c, r).ok()
            }
          })
          .collect();
        rows.push(row);
      }
    }

    let row_count = rows.len() as i64;
    Self {
      columns,
      rows,
      row_count,
    }
  }

}

/// Serialize a `QueryResult` (columns + rows) to a JSON string.
///
/// Provided as a free function because `#[napi(object)]` structs can't carry
/// their own `#[napi]` methods.
#[napi]
pub fn query_result_to_json(result: QueryResult) -> String {
  let cols = result
    .columns
    .iter()
    .map(|c| format!("\"{}\"", json_escape(c)))
    .collect::<Vec<_>>()
    .join(",");
  let rows = result
    .rows
    .iter()
    .map(|r| {
      let cells = r
        .iter()
        .map(|c| match c {
          Some(v) => format!("\"{}\"", json_escape(v)),
          None => "null".to_string(),
        })
        .collect::<Vec<_>>()
        .join(",");
      format!("[{cells}]")
    })
    .collect::<Vec<_>>()
    .join(",");
  format!("{{\"columns\":[{cols}],\"rows\":[{rows}]}}")
}

/// High-level handle to a DataFusion `SessionContext`.
///
/// Wrap an `Arc<SessionContext>` so it can be shared safely across the async
/// NAPI calls (DataFusion's methods take `&self`).
#[napi]
pub struct SessionContext {
  pub(crate) inner: Arc<DFSessionContext>,
}

#[napi]
impl SessionContext {
  /// Create a new, empty session.
  #[napi(constructor)]
  pub fn new() -> Self {
    Self {
      inner: Arc::new(DFSessionContext::new()),
    }
  }

  /// Register a CSV file as a table named `name`.
  ///
  /// `has_header` defaults to `true` (DataFusion's default) when `None`.
  /// `delimiter` defaults to `,` when `None`.
  #[napi]
  pub async fn register_csv(
    &self,
    name: String,
    path: String,
    has_header: Option<bool>,
    delimiter: Option<String>,
  ) -> napi::Result<()> {
    let mut options = CsvReadOptions::new();
    if let Some(header) = has_header {
      options = options.has_header(header);
    }
    if let Some(delim) = delimiter {
      let bytes = delim.as_bytes();
      if bytes.len() != 1 {
        return Err(napi::Error::new(
          Status::InvalidArg,
          "delimiter must be exactly one byte",
        ));
      }
      options = options.delimiter(bytes[0]);
    }
    self
      .inner
      .register_csv(&name, &path, options)
      .await
      .map_err(to_napi_err)
  }

  /// Register an Apache Parquet file as a table named `name`.
  #[napi]
  pub async fn register_parquet(&self, name: String, path: String) -> napi::Result<()> {
    self
      .inner
      .register_parquet(&name, &path, ParquetReadOptions::default())
      .await
      .map_err(to_napi_err)
  }

  /// Register an Apache Avro file as a table named `name`.
  #[napi]
  pub async fn register_avro(&self, name: String, path: String) -> napi::Result<()> {
    self
      .inner
      .register_avro(&name, &path, AvroReadOptions::default())
      .await
      .map_err(to_napi_err)
  }

  /// Register data sources via raw SQL, e.g. `CREATE EXTERNAL TABLE ...`.
  ///
  /// This is the same as [`SessionContext::execute`] but semantically intended
  /// for DDL. Any resulting output rows are returned as a [`QueryResult`]
  /// (empty for statements like `CREATE EXTERNAL TABLE`).
  #[napi]
  pub async fn register_sql(&self, sql: String) -> napi::Result<QueryResult> {
    self.execute(sql).await
  }

  /// Execute a SQL statement and collect the results.
  ///
  /// Works for both queries (`SELECT ...`) and DDL (`CREATE EXTERNAL TABLE`,
  /// `CREATE VIEW`, etc.). Returns a [`QueryResult`] with materialized rows.
  #[napi]
  pub async fn execute(&self, sql: String) -> napi::Result<QueryResult> {
    let df = self.inner.sql(&sql).await.map_err(to_napi_err)?;
    let batches = df.collect().await.map_err(to_napi_err)?;
    Ok(QueryResult::from_batches(batches))
  }
}
