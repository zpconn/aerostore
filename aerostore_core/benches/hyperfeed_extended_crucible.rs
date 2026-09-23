mod extended_crucible;

fn main() {
    if let Err(error) = extended_crucible::run() {
        eprintln!("extended-crucible: {error}");
        std::process::exit(2);
    }
}
