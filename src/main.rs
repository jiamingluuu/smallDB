use std::io::Write;

pub mod btree;
fn main() -> std::io::Result<()> {
    let mut buffer = String::new();

    loop {
        print_prompt();
        std::io::stdin().read_line(&mut buffer)?;
    }

    Ok(())
}

fn print_prompt() {
    print!("db >");
    let _ = std::io::stdout().flush();
}