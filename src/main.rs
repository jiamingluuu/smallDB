use crate::parser::*;

use std::io::Write;

mod parser;
mod row;

fn main() -> std::io::Result<()> {
    let mut buffer = String::new();

    loop {
        print_prompt();
        std::io::stdin().read_line(&mut buffer)?;
        let input = buffer.trim();

        if input.starts_with('.') {
            handle_meta_command(input);
        } else {
            match prepare_stmt(input) {
                Ok(stmt) => execute_stmt(stmt),
                Err(what) => println!("{what}"),
            };
        }
    }
}

fn print_prompt() {
    print!("db >");
    let _ = std::io::stdout().flush();
}

fn execute_stmt(stmt: Stmt) {
    todo!()
}