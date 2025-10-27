pub mod cache_system;
pub mod file_system;

struct IO_Manager {
    cache_sys: cache_system,
    file_sys: file_system,
}

impl IO_Manager {}
