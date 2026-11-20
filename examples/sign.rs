//! Mint one join token with the relay's own signer, for cross-checking other
//! implementations of the format.
//!
//! Usage: sign <secret> <room> <user> <display> <read|edit> <peer> <now>
//! Prints the token on stdout. Used to generate the fixture the Nextcloud app's
//! PHP signer must reproduce byte for byte.

#[allow(dead_code)]
#[path = "../src/token.rs"]
mod token;

fn main() {
    let a: Vec<String> = std::env::args().skip(1).collect();
    if a.len() != 7 {
        eprintln!("usage: sign <secret> <room> <user> <display> <read|edit> <peer> <now>");
        std::process::exit(2);
    }
    let now: i64 = match a[6].parse() {
        Ok(n) => n,
        Err(_) => {
            eprintln!("now must be an integer");
            std::process::exit(2);
        }
    };
    match token::sign_at(a[0].as_bytes(), &a[1], &a[2], &a[3], a[4] == "edit", &a[5], now) {
        Ok(t) => println!("{t}"),
        Err(e) => {
            eprintln!("sign failed: {e}");
            std::process::exit(1);
        }
    }
}
