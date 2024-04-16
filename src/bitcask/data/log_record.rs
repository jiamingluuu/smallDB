use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};

use crate::bitcask::data::data_file::CRC_LEN;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LogRecordType {
    Normal,
    Deleted,
}

/// On encoding, we formate the struct into the following format:
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
#[derive(Debug, PartialEq)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: LogRecordType, /* On deletion, change this attribute to DELTED.
                                            * Because we can not change the already written
                                            * records, so an identifier for deletion and writing
                                            * is required. */
}

#[derive(Clone, Copy)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) ofs: u64,
}

impl LogRecord {
    pub fn encode(&self) -> Vec<u8> {
        let (encoded_buf, _) = self.encode_and_get_crc();
        encoded_buf
    }

    pub fn get_crc(&self) -> u32 {
        let (_, crc) = self.encode_and_get_crc();
        crc
    }

    fn encode_and_get_crc(&self) -> (Vec<u8>, u32) {
        let mut buf = BytesMut::new();
        buf.reserve(self.get_encoded_record_length());

        // Append BUF with the encoded TYPE, KEY_SIZE, VALUE_SIZE, KEY, VALUE.
        buf.put_u8(self.record_type as u8);
        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // Append Buf with CRC.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc);

        (buf.to_vec(), crc)
    }

    /// Calculate the size of a LOG_RECORD after encoding.
    fn get_encoded_record_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + CRC_LEN
    }
}

impl LogRecordType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => LogRecordType::Normal,
            1 => LogRecordType::Deleted,
            _ => panic!("unknown log record type"),
        }
    }
}

pub fn max_log_record_header_size() -> usize {
    // MAX_SIZE = len(type) + len(key_size) + len(value_size)
    //          = len(u8) + len(u32) + len(u32)
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_encode() {
        let record1 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "Prince Hamlet".as_bytes().to_vec(),
            record_type: LogRecordType::Normal,
        };
        let encoded1 = record1.encode();
        assert!(encoded1.len() > 5);
        assert_eq!(2443068230, record1.get_crc());

        let record2 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: Default::default(),
            record_type: LogRecordType::Normal,
        };
        let encoded2 = record2.encode();
        assert!(encoded2.len() > 5);
        assert_eq!(2040151154, record2.get_crc());

        let record3 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "Prince Hamlet".as_bytes().to_vec(),
            record_type: LogRecordType::Deleted,
        };
        let encoded3 = record3.encode();
        assert!(encoded3.len() > 5);
        assert_eq!(4109989888, record3.get_crc());
    }
}
