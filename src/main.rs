use rand::{distributions::Alphanumeric, Rng};
use reqwest::{blocking, StatusCode};
use serde_json::{json, Value};
use std::process::Command;
use std::sync::{Arc, Barrier};

use crate::sendhelper::{send, send_body};

mod sendhelper;

fn make_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect::<String>()
}

fn dump_heap_profile(name: &str) {
    println!("Running memsnap.fish {} ...", name);
    let output = Command::new("memsnap.fish")
        .arg(name)
        .output()
        .expect("Could not execute memsnap.fish");
    println!("Output of memsnap: {:?}", output);
}

fn main() {
    println!("Hello, I am torturing your ArangoDB!");
    let args: Vec<String> = std::env::args().map(|s| s.to_string()).collect();
    if args.len() == 2 && args[1] == "--help" {
        println!("Usage: trxtorturer NR_TRX NR_BATCHES BATCH_SIZE DELAY_IN_SECONDS KEYSIZE");
        std::process::exit(0);
    }
    let nr_trx = if args.len() > 1 {
        args[1].parse::<usize>().expect("Need number of trxs")
    } else {
        100
    };
    let nr_batches = if args.len() > 2 {
        args[2].parse::<usize>().expect("Need number of batches")
    } else {
        15
    };
    let batch_size = if args.len() > 3 {
        args[3].parse::<usize>().expect("Need number of batches")
    } else {
        1000
    };
    let delay = if args.len() > 4 {
        args[4].parse::<u64>().expect("Need number for delay")
    } else {
        30
    };
    let keysize = if args.len() > 5 {
        args[5].parse::<usize>().expect("Need number for keysize")
    } else {
        80
    };
    println!("Number of transactions/threads: {}", nr_trx);
    println!("Number of batches: {}", nr_batches);
    println!("Batch size: {}", batch_size);
    println!("Delay in seconds: {}", delay);
    println!("Keysize in bytes: {}", keysize);

    let client = blocking::Client::new();

    dump_heap_profile("start");

    // Drop collection:
    println!("Dropping collection c...");
    send(&client, "DELETE", "/_api/collection/c", |_code| true);
    // Ignore errors.

    // Create collection:
    println!("Creating collection c...");
    let _ = send_body(
        &client,
        "POST",
        "/_api/collection/c",
        None,
        &json!({"name":"c"}),
        |c| c >= StatusCode::OK && c <= StatusCode::CREATED,
    );

    let barrier = Arc::new(Barrier::new(nr_trx + 1));
    let barrier2 = Arc::new(Barrier::new(nr_trx + 1));
    let barrier3 = Arc::new(Barrier::new(nr_trx + 1));

    // Create snapshot thread:
    let barrierclone = barrier.clone();
    let barrier2clone = barrier2.clone();
    let barrier3clone = barrier3.clone();
    let clientclone = client.clone();
    let j = std::thread::spawn(move || {
        // Other threads are building their transaction here
        barrierclone.wait();
        dump_heap_profile("largetrx");
        barrier2clone.wait();
        // Other threads are committing here
        barrier3clone.wait();

        // Drop collection:
        println!("Dropping collection c...");
        send(&clientclone, "DELETE", "/_api/collection/c", |_code| true);
        // Ignore errors.

        println!("Waiting for {}s for things to settle down...", delay);
        std::thread::sleep(std::time::Duration::from_secs(delay));
        // Wait until all is settled and RocksDB is no longer working
        dump_heap_profile("done");
    });
    // Create nr_trx threads:
    println!("Starting {} threads...", nr_trx);
    std::thread::scope(|s| {
        let mut v = vec![];
        for t in 0..nr_trx {
            let id = t;
            let barrier = barrier.clone();
            let barrier2 = barrier2.clone();
            let barrier3 = barrier3.clone();
            let client = client.clone();
            v.push(s.spawn(move || {
                // Run one transaction with many writes:
                let r = send_body(
                    &client,
                    "POST",
                    "/_api/transaction/begin",
                    None,
                    &json!({"collections":{"write":["c"]}}),
                    |c| c == StatusCode::CREATED,
                )
                .json::<Value>()
                .unwrap();
                let trxid = r["result"]["id"].as_str().unwrap();

                for _j in 0..nr_batches {
                    let mut l = vec![];
                    l.reserve(batch_size);
                    for k in 0..batch_size {
                        l.push(json!({ "_key": make_random_string(keysize),
                                       "Hallo": k,
                                       "s": make_random_string(100) }));
                    }
                    send_body(&client, "POST", "/_api/document/c", Some(trxid), &l, |c| {
                        c == StatusCode::ACCEPTED
                    });
                }
                println!("Thread {} done, transaction built.", id);
                barrier.wait();
                // Heap profile will be dumped here.
                barrier2.wait();
                println!("Thread {} sleeping for 1 s...", id);
                std::thread::sleep(std::time::Duration::from_secs(1));
                let url = "/_api/transaction/".to_string() + trxid;
                send_body(&client, "PUT", &url[..], None, &json!({}), |c| {
                    c == StatusCode::OK
                });
                println!("Thread {} done, transaction committed.", id);
                barrier3.wait();
                // Another heap profile will be dumped here.
            }));
        }
        while v.len() > 0 {
            let t = v.pop().unwrap();
            t.join().unwrap();
        }
    });
    j.join().unwrap();
}
