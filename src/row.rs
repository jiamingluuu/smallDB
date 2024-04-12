use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;
pub const ROW_SIZE: usize = USERNAME_SIZE + EMAIL_SIZE + 4;

#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    pub id: u32,

    #[serde(with = "serde_bytes")]
    pub username: [u8; USERNAME_SIZE],

    #[serde(with = "serde_bytes")]
    pub email: [u8; EMAIL_SIZE],
}

impl Row {
    pub fn new(_id: &str, _username: &str, _email: &str) -> Result<Row, String> {
        if _username.len() > USERNAME_SIZE {
            return Err(format!(
                "Username is too long. Received: {} bytes, 
                expect: {} bytes at most",
                _username.len(),
                USERNAME_SIZE
            ));
        }

        if _email.len() > EMAIL_SIZE {
            return Err(format!(
                "Email is too long. Received: {} bytes, expect: {} bytes at most",
                _email.len(),
                EMAIL_SIZE
            ));
        }

        let id = _id
            .parse::<u32>()
            .map_err(|_| "Invalid id provided".to_string())?;

        let mut username = [0; USERNAME_SIZE];
        for (i, c) in _username.bytes().enumerate() {
            username[i] += c;
        }

        let mut email = [0; EMAIL_SIZE];
        for (i, c) in _email.bytes().enumerate() {
            email[i] += c;
        }

        Ok(Row {
            id,
            username,
            email,
        })
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

impl str::FromStr for Row {
    type Err = String;

    fn from_str(row: &str) -> Result<Self, Self::Err> {
        let columns: Vec<&str> = row.split(' ').collect();
        match columns[..] {
            [id, username, email] => Ok(Self::new(id, username, email)?),
            _ => Err("Unacceptable pattern, expected: [id] [username] [email]".to_string()),
        }
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let username_end = self
            .username
            .iter()
            .position(|&x| x == 0)
            .unwrap_or(self.username.len());
        let email_end = self
            .email
            .iter()
            .position(|&x| x == 0)
            .unwrap_or(self.email.len());
        write!(
            f,
            "(id: {}, username: {}, email: {})",
            self.id,
            str::from_utf8(&self.username[..username_end]).unwrap(),
            str::from_utf8(&self.email[..email_end]).unwrap()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        let r1: Row = Row::new("1", "alice", "alice@example.com").unwrap();
        let r2: Row = Row::new("2", "bob", "bob@example.com").unwrap();

        assert_eq!(
            format!("{}", r1),
            "(id: 1, username: alice, email: alice@example.com)"
        )
    }
}
