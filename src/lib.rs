#[cfg(feature = "spark-shell")]
mod cli;
mod data_frame;
mod sql;
#[cfg(feature = "spark-connect")]
mod spark_connect;

use napi_derive::napi;

#[napi]
pub fn add(left: i32, right: i32) -> i32 {
  left + right
}
