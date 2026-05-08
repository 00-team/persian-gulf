pub mod logger;
pub mod shipment;
pub mod spring;
pub mod uid;
pub mod utils;

pub fn sys_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
