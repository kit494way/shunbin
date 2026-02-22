use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{PathBuf, is_separator};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use log::warn;
use serde::{Deserialize, Serialize, de};
use sudachi::dic::dictionary::JapaneseDictionary;
use sudachi_tantivy::SudachiTokenizer;
use tantivy::Index;
use tantivy::TantivyDocument;
use tantivy::Term;
use tantivy::directory::MmapDirectory;
use tantivy::schema::{DateOptions, IndexRecordOption, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::RawTokenizer;

use crate::config::{FieldConfig, SchemaConfig, TokenizerConfig};
use crate::env::data_dir;
use crate::fs::RecursiveReadDir;

const RAW_TOKENIZER_NAME: &str = "_raw";

#[derive(Debug)]
pub struct Indexer {
    tms: Option<TimestampManager>,
    count: usize,
    increment: bool,
}

impl Indexer {
    pub fn new() -> Self {
        let tms = match TimestampManager::new() {
            Ok(tms) => Some(tms),
            Err(e) => {
                warn!("Failed to initialize TimestampManager, {}", e);
                None
            }
        };
        Self {
            tms,
            count: 0,
            increment: true,
        }
    }

    pub fn index(
        &mut self,
        index_name: String,
        index: &tantivy::Index,
        sources: HashMap<String, PathBuf>,
    ) -> anyhow::Result<()> {
        let schema_fields = SchemaFields::from_index(index)?;
        let mut index_writer = index.writer(50_000_000)?;

        sources
            .iter()
            .try_for_each(|(source_name, source)| -> anyhow::Result<()> {
                let start_at = Utc::now();
                let mut count = 0;

                let read_dir =
                    self.read_source(source.clone(), source_name.clone(), index_name.clone())?;
                for entry in read_dir {
                    let path = entry?;
                    if self.index_inner(
                        &mut index_writer,
                        schema_fields,
                        source_name.clone(),
                        source.clone(),
                        path,
                    )? {
                        count += 1;
                    }
                }
                index_writer.commit()?;
                self.update_timestamp(index_name.clone(), source_name.clone(), start_at);
                self.count += count;

                Ok(())
            })?;

        Ok(())
    }

    pub fn index_file(
        &mut self,
        index: &tantivy::Index,
        sources: HashMap<String, PathBuf>,
        path: PathBuf,
    ) -> anyhow::Result<()> {
        let schema_fields = SchemaFields::from_index(index)?;
        let mut index_writer = index.writer(50_000_000)?;

        sources
            .iter()
            .try_for_each(|(source_name, source)| -> anyhow::Result<()> {
                if self.index_inner(
                    &mut index_writer,
                    schema_fields,
                    source_name.clone(),
                    source.clone(),
                    path.clone(),
                )? {
                    index_writer.commit()?;
                    self.count += 1;
                }

                Ok(())
            })?;

        Ok(())
    }

    fn index_inner(
        &self,
        index_writer: &mut tantivy::IndexWriter,
        schema: SchemaFields,
        source_name: String,
        source: PathBuf,
        path: PathBuf,
    ) -> anyhow::Result<bool> {
        let path_string = match path.to_str() {
            Some(s) => s.to_string(),
            None => {
                warn!("Skip {:?}, path string contains non-UTF8 string", path);
                return Ok(false);
            }
        };

        let relative_path = match source.to_str().and_then(|x| path_string.strip_prefix(x)) {
            Some(s) => s.trim_start_matches(is_separator),
            None => {
                warn!("Skip {path_string}, failed to get a relative path");
                return Ok(false);
            }
        };

        // Delete an old document
        let id = format!("{}:{}", source_name, relative_path);
        index_writer.delete_term(Term::from_field_text(schema.id, id.as_str()));

        let body = fs::read_to_string(path)?;
        if body.is_empty() {
            return Ok(false);
        }

        // Treat the first line as the title of the Markdown file and remove all leading # characters.
        let title = body.lines().nth(0).unwrap().trim_start_matches("#").trim();

        let mut doc = TantivyDocument::default();
        doc.add_text(schema.title, title);
        doc.add_text(schema.body, body);
        doc.add_text(schema.source, source_name);
        doc.add_text(schema.path, relative_path);

        let now = tantivy::DateTime::from_timestamp_secs(chrono::Utc::now().timestamp());
        doc.add_date(schema.updated_at, now);

        doc.add_text(schema.id, id);

        index_writer.add_document(doc)?;
        Ok(true)
    }

    pub fn indexed_count(&self) -> usize {
        self.count
    }

    pub fn set_increment(mut self, increment: bool) -> Self {
        self.increment = increment;
        self
    }

    pub fn is_incrementable(&self) -> bool {
        self.tms.is_some()
    }

    fn read_source(
        &mut self,
        source: PathBuf,
        source_name: String,
        index_name: String,
    ) -> io::Result<RecursiveReadDir> {
        let mut read_dir = RecursiveReadDir::new(source)?;
        if self.increment
            && let Some(tms) = self.tms.as_ref()
            && let Some(last_updated_at) = tms.get_timestamp(index_name, source_name)
        {
            read_dir = read_dir.updated_after(last_updated_at);
        }
        Ok(read_dir)
    }

    fn update_timestamp(
        &mut self,
        index_name: String,
        source_name: String,
        start_at: DateTime<Utc>,
    ) {
        if let Some(tms) = self.tms.as_mut() {
            tms.update(index_name.clone(), source_name.clone(), start_at)
                .unwrap_or_else(|e| {
                    warn!("Failed to update timestamp, {e:?}");
                });
        }
    }
}

#[derive(Copy, Clone, Debug)]
struct SchemaFields {
    title: tantivy::schema::Field,
    body: tantivy::schema::Field,
    source: tantivy::schema::Field,
    path: tantivy::schema::Field,
    updated_at: tantivy::schema::Field,
    id: tantivy::schema::Field,
}

impl SchemaFields {
    fn from_index(index: &tantivy::Index) -> anyhow::Result<Self> {
        let schema = index.schema();
        Ok(SchemaFields {
            title: schema.get_field("title")?,
            body: schema.get_field("body")?,
            source: schema.get_field("source")?,
            path: schema.get_field("path")?,
            updated_at: schema.get_field("updated_at")?,
            id: schema.get_field("id")?,
        })
    }
}

fn register_tokenizer(
    index: &tantivy::index::Index,
    tokenizer_name: &str,
    config: TokenizerConfig,
) -> anyhow::Result<()> {
    match config {
        TokenizerConfig::Sudachi {
            dict,
            user_dict: _,
            mode,
        } => {
            let sudach_config = sudachi::config::Config::new(None, None, Some(dict))?;
            let jp_dict = JapaneseDictionary::from_cfg(&sudach_config)?;
            let dict = Arc::new(jp_dict);
            let mut tokenizer = SudachiTokenizer::new(dict);
            tokenizer.set_mode(mode.into());
            index.tokenizers().register(tokenizer_name, tokenizer);
        }
        TokenizerConfig::Raw => index
            .tokenizers()
            .register(tokenizer_name, RawTokenizer::default()),
    }

    Ok(())
}

fn create_text_option(field: &FieldConfig) -> TextOptions {
    let text_index_options =
        TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqsAndPositions);

    let text_index_options = if field.tokenizer.is_empty() {
        // Use default tokenizer
        text_index_options
    } else {
        text_index_options.set_tokenizer(field.tokenizer.as_str())
    };

    TextOptions::default().set_indexing_options(text_index_options)
}

fn create_schema(config: SchemaConfig) -> tantivy::schema::Schema {
    let mut scheme_builder = tantivy::schema::Schema::builder();

    scheme_builder.add_text_field(
        "title",
        create_text_option(&config.fields.title.unwrap_or_default()).set_stored(),
    );
    scheme_builder.add_text_field(
        "body",
        create_text_option(&config.fields.body.unwrap_or_default()),
    );

    scheme_builder.add_text_field(
        "source",
        TextOptions::default()
            .set_indexing_options(TextFieldIndexing::default().set_tokenizer(RAW_TOKENIZER_NAME))
            .set_stored(),
    );
    scheme_builder.add_text_field(
        "path",
        TextOptions::default()
            .set_indexing_options(TextFieldIndexing::default().set_tokenizer(RAW_TOKENIZER_NAME))
            .set_stored(),
    );
    scheme_builder.add_date_field(
        "updated_at",
        DateOptions::from(tantivy::schema::INDEXED)
            .set_stored()
            .set_fast(),
    );
    scheme_builder.add_text_field(
        "id",
        TextOptions::default()
            .set_indexing_options(TextFieldIndexing::default().set_tokenizer(RAW_TOKENIZER_NAME))
            .set_stored(),
    );

    scheme_builder.build()
}

pub fn create_index(
    index_path: PathBuf,
    schema_config: SchemaConfig,
    tokenizers: HashMap<String, TokenizerConfig>,
) -> anyhow::Result<Index> {
    let schema = create_schema(schema_config);

    fs::create_dir_all(&index_path)?;
    let dir = MmapDirectory::open(index_path)?;
    let index = Index::open_or_create(dir, schema)?;

    tokenizers
        .into_iter()
        .try_for_each(|(name, tokenizer_config)| {
            register_tokenizer(&index, name.as_str(), tokenizer_config)
        })?;

    index
        .tokenizers()
        .register(RAW_TOKENIZER_NAME, RawTokenizer::default());

    Ok(index)
}

#[derive(Debug, Deserialize, Serialize)]
struct TimestampManager {
    timestamps: HashMap<TimestampKey, DateTime<Utc>>,
}

impl TimestampManager {
    const TIMESTAMP_FILE_NAME: &str = "timestamp.toml";

    fn new() -> anyhow::Result<Self> {
        let timestamp_path = data_dir()?.join(Self::TIMESTAMP_FILE_NAME);
        if timestamp_path.exists() {
            let contents = fs::read_to_string(&timestamp_path).map_err(|e| {
                warn!(
                    "Failed to read {}: {:?}",
                    timestamp_path.to_string_lossy(),
                    e
                );
                e
            })?;
            toml::from_str::<TimestampManager>(&contents).map_err(anyhow::Error::from)
        } else {
            Ok(Self {
                timestamps: HashMap::<TimestampKey, DateTime<Utc>>::new(),
            })
        }
    }

    fn update(
        &mut self,
        index: String,
        source: String,
        datetime: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.timestamps
            .insert(TimestampKey(index, source), datetime);
        self.save()
    }

    fn save(&self) -> anyhow::Result<()> {
        let contents = toml::to_string(self)?;
        let timestamp_path = data_dir()?.join(Self::TIMESTAMP_FILE_NAME);
        fs::write(timestamp_path, contents)?;
        Ok(())
    }

    fn get_timestamp(&self, index: String, source: String) -> Option<DateTime<Utc>> {
        self.timestamps.get(&TimestampKey(index, source)).copied()
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TimestampKey(String, String);

impl Serialize for TimestampKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{}:{}", self.0, self.1))
    }
}

impl<'de> Deserialize<'de> for TimestampKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let parts: Vec<&str> = s.splitn(2, ":").collect();
        if parts.len() != 2 {
            return Err(de::Error::custom(
                "expected 'index_name:source_name' format",
            ));
        }
        Ok(TimestampKey(parts[0].to_string(), parts[1].to_string()))
    }
}
