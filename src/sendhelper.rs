use reqwest::{blocking, StatusCode};
use serde_json::Value;

const BASE_URL: &str = "http://localhost:8529";

pub fn send_body<T: serde::ser::Serialize>(
    client: &blocking::Client,
    verb: &str,
    path: &str,
    trx: Option<&str>,
    body: &T,
    expected: fn(c: StatusCode) -> bool,
) -> reqwest::blocking::Response {
    let url = BASE_URL.to_string() + path;
    let mut req = match verb {
        "POST" => client.post(url),
        "PUT" => client.put(url),
        _ => panic!("Panic"),
    };
    if trx.is_some() {
        req = req.header("x-arango-trx-id", trx.unwrap());
    }
    let resp = req.body(serde_json::to_vec(&body).unwrap()).send().unwrap();
    if expected(resp.status()) {
        return resp;
    }
    let body = resp.json::<Value>().unwrap();
    eprintln!(
        "Error in create collection request: {}",
        serde_json::to_string(&body).unwrap()
    );
    std::process::exit(1);
}

pub fn send(
    client: &blocking::Client,
    verb: &str,
    path: &str,
    expected: fn(c: StatusCode) -> bool,
) -> reqwest::blocking::Response {
    let url = BASE_URL.to_string() + path;
    let resp = match verb {
        "DELETE" => client.delete(url).send().unwrap(),
        "GET" => client.get(url).send().unwrap(),
        _ => panic!("Panic"),
    };
    if expected(resp.status()) {
        return resp;
    }
    let body = resp.json::<Value>().unwrap();
    eprintln!(
        "Error in request: {}",
        serde_json::to_string(&body).unwrap()
    );
    std::process::exit(1);
}
