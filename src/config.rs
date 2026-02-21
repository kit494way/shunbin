use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

use crate::env::{config_dir, data_dir};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub default_opts: Option<DefaultOptsConfig>,
    pub indexes: HashMap<String, IndexConfig>,
    pub schema: HashMap<String, SchemaConfig>,
    pub tokenizers: HashMap<String, TokenizerConfig>,
}

impl Config {
    pub fn get_default_search_index_name(&self) -> Result<String, ConfigError> {
        match self
            .default_opts
            .as_ref()
            .and_then(|x| x.search.as_ref())
            .and_then(|x| x.index.clone())
        {
            Some(x) => Ok(x),
            None => {
                if self.indexes.len() == 1 {
                    Ok(self.indexes.iter().next().unwrap().0.clone())
                } else {
                    Err(ConfigError::NoDefaultIndexName.into())
                }
            }
        }
    }

    pub fn get_default_search_limit(&self) -> usize {
        self.default_opts
            .as_ref()
            .and_then(|x| x.search.as_ref())
            .and_then(|x| x.limit)
            .unwrap_or(10)
    }

    pub fn get_schema(&self, name: &str) -> Result<SchemaConfig, ConfigError> {
        self.schema
            .get(name)
            .map(|x| x.clone())
            .ok_or_else(|| ConfigError::NotFoundSchema {
                schema_name: name.to_string(),
            })
    }

    pub fn load(config_path: &Path) -> Result<Config, ConfigError> {
        let content = fs::read_to_string(config_path)?;
        toml::from_str(content.as_str()).map_err(ConfigError::ParseError)
    }
}

#[derive(Debug, Deserialize)]
pub struct DefaultOptsConfig {
    search: Option<DefaultSearchOpts>,
}

#[derive(Debug, Deserialize)]
pub struct DefaultSearchOpts {
    pub index: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct IndexConfig {
    pub path: Option<PathBuf>,
    pub schema: String,
    pub sources: HashMap<String, PathBuf>,
}

impl IndexConfig {
    pub fn get_path(&self, index_name: &str) -> anyhow::Result<PathBuf> {
        match self.path.clone() {
            Some(x) => Ok(x),
            None => data_dir().map(|x| x.join("indexes").join(index_name)),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct SchemaConfig {
    pub fields: FieldsConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct FieldsConfig {
    pub body: Option<FieldConfig>,
    pub title: Option<FieldConfig>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct FieldConfig {
    pub tokenizer: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "tokenizer", rename_all = "lowercase")]
pub enum TokenizerConfig {
    Sudachi {
        dict: PathBuf,
        #[allow(dead_code)]
        user_dict: Option<PathBuf>,
        mode: SudachiSplitMode,
    },
    Raw,
}

#[derive(Clone, Debug, Deserialize)]
pub enum SudachiSplitMode {
    A,
    B,
    C,
}

impl Into<sudachi::analysis::Mode> for SudachiSplitMode {
    fn into(self) -> sudachi::analysis::Mode {
        match self {
            SudachiSplitMode::A => sudachi::analysis::Mode::A,
            SudachiSplitMode::B => sudachi::analysis::Mode::B,
            SudachiSplitMode::C => sudachi::analysis::Mode::C,
        }
    }
}

pub fn get_default_config_path() -> anyhow::Result<PathBuf> {
    config_dir().map(|x| x.join("config.toml"))
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{0}")]
    IOError(#[from] std::io::Error),
    #[error("Not found schema '{schema_name}'")]
    NotFoundSchema { schema_name: String },
    #[error("{0}")]
    ParseError(#[from] toml::de::Error),
    #[error("Not found default index name")]
    NoDefaultIndexName,
}
