use prost::length_delimiter_len;

#[derive(PartialEq)]
pub enum LogRecordType {
    Normal,
    Deleted,
}

/// Entry for keydir.
/// A log record has the format:
///
///        +------+----------+------------+----------+-------------------------+-----+
///        | Type | key_size | value_size |    key   |         value           | CRC |
///        +------+----------+------------+----------+-------------------------+-----+
///
///        |------------------------------|
///                     header
///
/// Remark:
/// In bitcask's original essay, CRC is at the beginng of a log record. Whereas for convenience,
/// I put it at the end, which has no effects on the implementation, nor the performace.
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

    pub fn get_crc(&mut self) -> u32 {
        todo!()
    }
}

impl LogRecordType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => LogRecordType::Normal,
            2 => LogRecordType::Deleted,
            _ => panic!("unknown log record type"),
        }
    }
}

pub fn max_log_record_header_size() -> usize {
    // MAX_SIZE = len(type) + len(key_size) + len(value_size)
    //          = len(u8) + len(u32) + len(u32)
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}
