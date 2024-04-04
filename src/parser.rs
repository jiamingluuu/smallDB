use std::str::FromStr;
use std::process::exit;

pub enum MetaCommand {
    Exit,
}

pub enum StmtType {
    Insert,
    Select,
}

pub struct Stmt {
    stmt_type: StmtType,
}

impl FromStr for MetaCommand {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            ".exit" => Ok(MetaCommand::Exit),
            _ => Err(format!("Unrecognized command {}", input)),
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

pub fn handle_meta_command(input: &str) {
    match MetaCommand::from_str(input) {
        Ok(command) => execute_meta_command(command),
        Err(what) => println!("{what}"),
    };
}

fn execute_meta_command(command: MetaCommand) {
    match command {
        MetaCommand::Exit => exit(0),
    }
}

pub fn prepare_stmt(input: &str) -> Result<Stmt, String> {
    match input.to_lowercase().split_once(' ') {
        None => todo!(),
        Some((action, args)) => {
            let stmt_type = StmtType::from_str(action)?;
            Ok(Stmt { stmt_type })
        }
    }
}