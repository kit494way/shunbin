mod config;
mod env;
mod fs;
mod index;
mod path;
mod search;

use std::path::PathBuf;
use std::{process, usize};

use clap::{Args, Parser, Subcommand};
use log::{debug, error, warn};

use crate::config::{Config, get_default_config_path};
use crate::index::{Indexer, create_index};
use crate::search::search;

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, short = 'c', global = true)]
    config: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Commands {
    Index {
        #[arg(long, short = 'i')]
        indexes: Vec<String>,

        #[command(flatten)]
        index_mode: IndexMode,
    },
    Search {
        #[arg(long, short = 'i')]
        index: Option<String>,

        #[arg(long, short = 'l')]
        limit: Option<usize>,

        query: Vec<String>,
    },
}

#[derive(Args, Debug)]
#[group(required = false, multiple = false)]
struct IndexMode {
    #[arg(long)]
    full: bool,

    #[arg(long)]
    increment: bool,
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    let config_path = cli.config.unwrap_or_else(|| {
        get_default_config_path().unwrap_or_else(|e| {
            error!("Failed to get the config file path, {e}");
            process::exit(1);
        })
    });
    let config = Config::load(config_path.as_path()).unwrap_or_else(|e| {
        error!("Failed to load {:?}, {}.", config_path, e);
        process::exit(1);
    });

    match &cli.command {
        Commands::Index {
            indexes,
            index_mode,
        } => {
            let mut indexer = Indexer::new();
            indexer = match (
                index_mode.full,
                index_mode.increment,
                indexer.is_incrementable(),
            ) {
                (true, _, _) => indexer.set_increment(false),
                (_, true, true) => indexer.set_increment(true),
                (_, true, false) => {
                    error!("Cannot execute incremental index.");
                    process::exit(1);
                }
                (_, false, true) => indexer,
                (_, false, false) => {
                    warn!("Fall back to full index.");
                    indexer.set_increment(false)
                }
            };

            config
                .indexes
                .iter()
                .filter(|x| indexes.is_empty() || indexes.contains(&x.0))
                .try_for_each(|(index_name, index_config)| {
                    let schema_config = config.get_schema(index_config.schema.as_str())?;
                    let index_path = index_config.get_path(index_name)?;
                    let index =
                        &create_index(index_path, schema_config, config.tokenizers.clone())?;
                    indexer
                        .index(index_name.to_string(), index, index_config.sources.clone())
                        .map(|_| eprintln!("{} documents were indexed.", indexer.indexed_count()))
                })?;
        }
        Commands::Search {
            index,
            limit,
            query,
        } => {
            // Determine the target index in the following order:
            // 1. Command line argument
            // 2. `default_opts.search.index` in the config file
            // 3. If there is only one entry in `indexes` in the config file, use that
            // 4. Error
            let index_name = &index.clone().unwrap_or_else(|| {
                config.get_default_search_index_name().unwrap_or_else(|e| {
                    error!("Please specify the index to search, {}", e);
                    process::exit(1);
                })
            });

            let index_config = config.indexes.get(index_name).unwrap_or_else(|| {
                error!("Failed to get the index config named '{}'.", index_name);
                process::exit(1);
            });
            let schema_config = config.get_schema(index_config.schema.as_str())?;
            let index_path = index_config.get_path(index_name)?;
            let index = &create_index(index_path, schema_config, config.tokenizers.clone())?;

            let limit = limit.unwrap_or_else(|| config.get_default_search_limit());
            let docs = search(index, query.join(" ").as_str(), limit)?;

            docs.into_iter().try_for_each(|doc| -> anyhow::Result<()> {
                let doc_path = match doc.absolute_path(&index_config.sources) {
                    Ok(x) => x.to_string_lossy().to_string(),
                    Err(e) => {
                        error!("{}", e);
                        return Ok(());
                    }
                };

                debug!("{:?}", doc);

                println!(
                    "{}, {}, {}",
                    doc.title,
                    doc.updated_at.to_rfc3339(),
                    doc_path,
                );

                Ok(())
            })?;
        }
    };

    Ok(())
}
