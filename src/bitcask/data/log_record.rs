#[derive(Clone, Copy)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) ofs: u64,
}
