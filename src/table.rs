use crate::row::{Row, ROW_SIZE};

use bincode;

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;
pub const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

#[derive(Debug)]
pub enum ExecuteError {
    TableFull,
    SerializationError,
    DeserializeError,
}

pub struct Table {
    pub num_rows: usize,
    pub pages: [[u8; PAGE_SIZE]; TABLE_MAX_PAGES],
}

impl Table {
    pub fn new() -> Table {
        Table {
            num_rows: 0,
            pages: [[0; PAGE_SIZE]; TABLE_MAX_PAGES],
        }
    }
}

impl Table {
    pub fn insert(&mut self, row: &Row) -> Result<(), ExecuteError> {
        if self.num_rows >= TABLE_MAX_ROWS {
            return Err(ExecuteError::TableFull);
        }

        let page_num = self.num_rows / ROWS_PER_PAGE;
        let row_offset = self.num_rows % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        if let Ok(row_) = serde_json::to_vec(row) {
            for i in byte_offset..byte_offset + ROW_SIZE {
                self.pages[page_num][i] = row_[i];
            }
        } else {
            return Err(ExecuteError::SerializationError);
        }

        self.num_rows += 1;

        Ok(())
    }

    pub fn select(&self) -> Result<(), ExecuteError> {
        for _ in 0..self.num_rows {
            let page_num = self.num_rows / ROWS_PER_PAGE;
            let row_offset = self.num_rows % ROWS_PER_PAGE;
            let byte_offset = row_offset * ROW_SIZE;

            let bytes = &self.pages[page_num][byte_offset..byte_offset + ROW_SIZE];
            if let Ok(row) = bincode::deserialize::<Row>(bytes) {
                println!("{}", row);
            } else {
                return Err(ExecuteError::DeserializeError);
            }
        }
        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     const t: Table = Table::new();
//     const r1: Row = Row::new("1", "alice", "alice@example.com").unwrap();
//     const r2: Row = Row::new("2", "bob", "bob@example.com").unwrap();
// }
