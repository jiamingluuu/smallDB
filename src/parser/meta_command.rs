use std::process::exit;
use std::str::FromStr;

#[derive(Debug, PartialEq)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_ok() {
        let input1 = ".exit";
        let output1 = MetaCommand::from_str(input1).unwrap();
        let expected1 = MetaCommand::Exit;
        assert_eq!(output1, expected1);
    }

    #[test]
    fn test_from_str_err() {
        let input2 = "dum";
        let output2 = MetaCommand::from_str(input2).unwrap_err();
        let expected2 = "Unrecognized command dum";
        assert_eq!(output2, expected2);
    }
}
