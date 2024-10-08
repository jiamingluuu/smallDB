use bytes::{BufMut, BytesMut};
use prost::{
    encode_length_delimiter,
    encoding::{decode_varint, encode_varint},
    length_delimiter_len,
};

use crate::data::data_file::CRC_LEN;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LogRecordType {
    Normal,
    Deleted,
    TxnFinished,
}

/// On encoding, we formate the struct into the following format:
/// ```
///  +------+----------+------------+----------+-------------------------+-----+
///  | Type | key_size | value_size |    key   |         value           | CRC |
///  +------+----------+------------+----------+-------------------------+-----+
///
///  |------------------------------|
///               header
/// ```
/// Remark:
/// In bitcask's original essay, CRC is at the beginning of a log record. Whereas for convenience,
/// I put it at the end, which has no effects on the implementation, nor the performance.
#[derive(Debug, PartialEq)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: LogRecordType, /* On deletion, change this attribute to DELETED.
                                            * Because we can not change the already written
                                            * records, so an identifier for deletion and writing
                                            * is required. */
}

pub struct TransactionRecord {
    pub(crate) record: LogRecord,
    pub(crate) pos: LogRecordPos,
}

/// struct used for log record lookup within a data file, where:
#[derive(Clone, Copy)]
pub struct LogRecordPos {
    /// The identifier of the file read.
    pub(crate) file_id: u32,

    /// The offset of log record to be looked up.
    pub(crate) ofs: u64,

    /// The size of log record on disk.
    pub(crate) size: u32,
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
            2 => LogRecordType::TxnFinished,
            _ => panic!("unknown log record type"),
        }
    }
}

impl LogRecordPos {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        encode_varint(self.file_id as u64, &mut buf);
        encode_varint(self.ofs, &mut buf);
        encode_varint(self.size as u64, &mut buf);
        buf.to_vec()
    }
}

pub fn decode_log_record_pos(pos: Vec<u8>) -> LogRecordPos {
    let mut buf = BytesMut::new();
    buf.put_slice(&pos);
    let fid = match decode_varint(&mut buf) {
        Ok(fid) => fid,
        Err(e) => panic!("decode log record pos Error: {}", e),
    };
    let ofs = match decode_varint(&mut buf) {
        Ok(ofs) => ofs,
        Err(e) => panic!("decode log record pos Error: {}", e),
    };
    let size = match decode_varint(&mut buf) {
        Ok(size) => size,
        Err(e) => panic!("decode log record pos Error: {}", e),
    };
    LogRecordPos {
        file_id: fid as u32,
        ofs,
        size: size as u32,
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
