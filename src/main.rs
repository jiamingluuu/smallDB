#[allow(dead_code)]
use crate::query_handler::*;
use crate::stmt::prepare_stmt;
use crate::table::Table;

use std::io::Write;

mod meta_command;
mod query_handler;
mod row;
mod stmt;
mod table;

fn main() -> std::io::Result<()> {
    let mut buffer = String::new();
    let mut table = Table::new();

    loop {
        print_prompt();
        std::io::stdin().read_line(&mut buffer)?;
        let input = buffer.trim();

        if input.starts_with('.') {
            handle_meta_command(input);
        } else {
            match prepare_stmt(input) {
                Ok(stmt) => execute_stmt(&stmt, &mut table),
                Err(what) => println!("{what}"),
            };
        }
    }
}

fn print_prompt() {
    print!("db >");
    let _ = std::io::stdout().flush();
}
