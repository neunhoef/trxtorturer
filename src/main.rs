use rand::{distributions::Alphanumeric, Rng};
use reqwest::{blocking, StatusCode};
use serde_json::{json, Value};
use std::sync::{Arc, Barrier};

use crate::sendhelper::{send, send_body};

const NR_TRX: usize = 100;
const NR_BATCHES: usize = 15;
const BATCH_SIZE: usize = 1000;

mod sendhelper;

fn make_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect::<String>()
}

fn main() {
    println!("Hello, I am torturing your ArangoDB!");
    let client = blocking::Client::new();

    // Drop collection:
    send(&client, "DELETE", "/_api/collection/c", |_code| true);
    // Ignore errors.

    // Create collection:
    let _ = send_body(
        &client,
        "POST",
        "/_api/collection/c",
        None,
        &json!({"name":"c"}),
        |c| c >= StatusCode::OK && c <= StatusCode::CREATED,
    );

    let barrier = Arc::new(Barrier::new(NR_TRX));

    // Create NR_TRX threads:
    std::thread::scope(|s| {
        let mut v = vec![];
        for t in 0..NR_TRX {
            let id = t;
            let barrier = barrier.clone();
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

                for _j in 0..NR_BATCHES {
                    let mut l = vec![];
                    l.reserve(BATCH_SIZE);
                    for k in 0..BATCH_SIZE {
                        l.push(json!({ "Hallo": k,
                                       "s": make_random_string(100) }));
                    }
                    send_body(&client, "POST", "/_api/document/c", Some(trxid), &l, |c| {
                        c == StatusCode::ACCEPTED
                    });
                }
                println!("Thread {} done, transaction built.", id);
                barrier.wait();
                println!("Thread {} sleeping for 10 s...", id);
                std::thread::sleep(std::time::Duration::from_secs(10));
                let url = "/_api/transaction/".to_string() + trxid;
                send_body(&client, "PUT", &url[..], None, &json!({}), |c| {
                    c == StatusCode::OK
                });
                println!("Thread {} done, transaction committed.", id);
            }));
        }
        while v.len() > 0 {
            let t = v.pop().unwrap();
            t.join().unwrap();
        }
    });
}
