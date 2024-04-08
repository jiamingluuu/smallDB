use crate::row::Row;

use std::str::FromStr;

pub enum StmtType {
    Insert,
    Select,
}

pub struct Stmt {
    pub stmt_type: StmtType,
    pub row: Option<Row>,
}

pub fn prepare_stmt(input: &str) -> Result<Stmt, String> {
    match input.to_lowercase().split_once(' ') {
        None => todo!(),
        Some((action, rest)) => {
            let stmt_type = StmtType::from_str(action)?;
            let row = match stmt_type {
                StmtType::Insert => Some(Row::from_str(rest)?),
                StmtType::Select => None,
            };

            Ok(Stmt { stmt_type, row })
        }
    }
}

impl FromStr for StmtType {
    type Err = String;

    fn from_str(action: &str) -> Result<Self, Self::Err> {
        match action {
            "insert" => Ok(StmtType::Insert),
            "select" => Ok(StmtType::Select),
            _ => Err(format!("Unrecognized keyword at start of {}", action)),
        }
    }
}
