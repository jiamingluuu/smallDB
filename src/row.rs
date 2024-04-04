const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;

pub struct Row {
    id: u32,
    username: [u8; USERNAME_SIZE],
    email: [u8; EMAIL_SIZE],
}
