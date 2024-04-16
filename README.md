# smallDB
My introductory project that is used for learn database design basics and practice my skills on writing Rust.

## Current progress
Now, I am implementing a KV data storage prototype by using [Bitcask](https://riak.com/assets/bitcask-intro.pdf).

## Objectives
If possible, I would like to extend the program into 
1. [Data Structure] Develop the indexer using B+-tree and skip list.
2. [Distributed System] Enable multiple nodes accessing the bitcask instance at the same time.
    - Use HTTP to enable data sharing.
    - Complete [MIT 6.824](https://pdos.csail.mit.edu/6.824/schedule.html), and bring usefulness of the [Raft consensus algorithm](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf) into my design 


## Example
```rs
use bytes::Bytes;
use smallDB::bitcask::{db, options::Options};

fn main() {
    let opts = Options::default();
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
}
```