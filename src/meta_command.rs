use std::process::exit;
use std::str::FromStr;

pub enum MetaCommand {
    Exit,
}

impl MetaCommand {
    pub fn execute(&self) {
        match self {
            MetaCommand::Exit => exit(0),
        }
    }
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

