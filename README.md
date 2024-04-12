# smallDB
My introductory project that is used for practice my skills on writing Rust.

## Current progress
Now, I am implementing a KV data storage prototype by using [Bitcask](https://riak.com/assets/bitcask-intro.pdf).

## Objectives
If possible, I would like to extend the program into 
1. Store all the bytes array in a multi-level B+ tree
2. Make a database concurrent, and can share stored data within multiple user process
