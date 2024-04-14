#[derive(PartialEq)]
pub enum LogRecordType {
    Normal,
    Deleted,
}

/// Entry for keydir
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: LogRecordType, /* On deletion, change this attribute to DELTED.
                                            * Because we can not change the already written
                                            * records, so an identifer for deletion and writing
                                            * is required. */
}

#[derive(Clone, Copy)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) ofs: u64,
}

impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        todo!()
    }
}
