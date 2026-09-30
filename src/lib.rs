use napi_derive::napi;

#[napi]
pub fn add(left: i32, right: i32) -> i32 {
  left + right
}
