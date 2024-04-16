use bytes::Bytes;

fn get_test_key(key: i32) -> Bytes {
    Bytes::from(std::format!("bitcask-key{:09}", key))
}

fn get_test_value(value: i32) -> Bytes {
    Bytes::from(std::format!("bitcask-key{:09}", value))
}

#[test]
fn test_get_test_key() {
    for key in 0..=10 {
        assert!(get_test_key(key).len() > 0)
    }

    for value in 0..=10 {
        assert!(get_test_value(value).len() > 0)
    }
}
