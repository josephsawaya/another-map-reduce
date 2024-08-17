use clap::{Parser, Subcommand};

pub mod component;
pub mod coordinator;
pub mod rpc;
pub mod shared;
pub mod worker;

use crate::component::Component;

#[derive(Debug, Parser)] // requires `derive` feature
#[command(name = "amr")]
#[command(about = "Another map reduce", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Coordinator {
        #[arg(short, long, default_value_t = 1)]
        num_buckets: u8,
    },
    Worker {
        #[arg(short, long, default_value_t = 1)]
        id: u8,
    },
}

fn main() {
    let args = Cli::parse();
    match args.command {
        Commands::Coordinator { num_buckets } => {
            let c = coordinator::Coordinator::new(num_buckets);
            c.start();
        }
        Commands::Worker { id } => {
            let w = worker::Worker { worker_id: id };
            w.start();
        }
    }
}
