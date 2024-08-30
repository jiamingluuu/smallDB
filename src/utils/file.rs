use std::path::PathBuf;

/// Size of the directory path.
pub fn dir_disk_size(dir_path: &PathBuf) -> u64 {
    if let Ok(size) = fs_extra::dir::get_size(dir_path) {
        return size;
    }
    return 0;
}

pub fn available_disk_size() -> u64 {
    if let Ok(size) = fs2::available_space(PathBuf::from("/")) {
        return size;
    }
    return 0;
}
