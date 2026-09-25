#![recursion_limit = "256"]

#[allow(dead_code, unused_imports)]
mod contention_crucible;
#[allow(dead_code, unused_imports)]
mod extended_crucible;

fn main() {
    if let Err(error) = contention_crucible::run() {
        eprintln!("contention-crucible: {error}");
        std::process::exit(2);
    }
}
