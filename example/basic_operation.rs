use std::fs;

use bytes::Bytes;
use smallDB::{db, options::Options};

fn main() {
    let opts = Options::default();
    let dir_path = opts.dir_path.clone(); // manually clean up the legacy
    let engine = db::Engine::open(opts).expect("failed to open bitcask engine");
    let put_res1 = engine.put(
        Bytes::from("quote"),
        Bytes::from("Shall I compare thee to a summer day."),
    );
    assert!(put_res1.is_ok());
    let get_res1 = engine.get(Bytes::from("quote"));
    assert!(get_res1.is_ok());
    let val = get_res1.ok().unwrap();
    println!("val = {:?}", String::from_utf8(val.to_vec()));
    fs::remove_dir_all(dir_path.clone())
        .expect(format!("Failed to remove enging data directory {:?}", dir_path).as_str());
}
