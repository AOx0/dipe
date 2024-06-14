use clap::Parser;
use strings::*;

#[derive(Parser)]
struct Args {
    input: Vec<String>,
}

fn main() {
    let Args { input } = Args::parse();
    let contents = input.join(" ");

    let words = sanitize(&contents, &[',']);

    println!("{}", String::from_iter(words))
}
