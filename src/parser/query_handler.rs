use std::str::FromStr;

use crate::parser::meta_command::MetaCommand;
use crate::parser::stmt::{Stmt, StmtType};
use crate::table::Table;

pub fn handle_meta_command(input: &str) {
    match MetaCommand::from_str(input) {
        Ok(command) => command.execute(),
        Err(what) => println!("{what}"),
    };
}

pub fn execute_stmt(stmt: &Stmt, table: &mut Table) {
    match stmt.stmt_type {
        StmtType::Insert => {
            if let Err(e) = table.insert(stmt.row.as_ref().unwrap()) {
                println!("{:?}", e);
            }
        }
        StmtType::Select => {
            if let Err(e) = table.select() {
                println!("{:?}", e);
            }
        }
    }
}
