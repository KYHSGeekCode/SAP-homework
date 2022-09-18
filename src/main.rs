#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]

//! The [`main`] function either checks the transaction model, or spawns a web server for the user
//! to explore the state space.

mod transaction;
mod transaction_model;

use transaction_model::TransactionModel;

use std::env;
use std::num::NonZeroUsize;
use std::thread::available_parallelism;

use stateright::{Checker, Model};

fn main() {
    let num_cpus = available_parallelism().map_or(1, NonZeroUsize::get);
    let mut args = env::args();
    let sub_command = args.nth(1);
    let model_checker = TransactionModel::new(3).checker();

    match sub_command.as_deref() {
        Some("check") => {
            println!("Model-check the transaction implementation.",);
            model_checker
                .threads(num_cpus)
                .spawn_dfs()
                .report(&mut std::io::stdout());
        }
        Some("explore") => {
            let address = args.next().unwrap_or_else(|| "localhost:3000".to_string());
            println!(
                "Explore the state space for the transaction model on {}.",
                address
            );
            model_checker.threads(num_cpus).serve(address);
        }
        _ => {
            println!("USAGE:");
            println!("  cargo run check");
            println!("  cargo run explore [host:port]");
        }
    }
}
