use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf, is_separator};
use std::sync::Arc;

use log::warn;
use sudachi::dic::dictionary::JapaneseDictionary;
use sudachi_tantivy::SudachiTokenizer;
use tantivy::Index;
use tantivy::TantivyDocument;
use tantivy::Term;
use tantivy::directory::MmapDirectory;
use tantivy::schema::{DateOptions, IndexRecordOption, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::RawTokenizer;

use crate::config::{FieldConfig, SchemaConfig, TokenizerConfig};

const RAW_TOKENIZER_NAME: &str = "_raw";

pub fn index(index: &tantivy::Index, sources: HashMap<String, PathBuf>) -> anyhow::Result<()> {
    let schema = index.schema();
    let field_title = schema.get_field("title")?;
    let field_body = schema.get_field("body")?;
    let field_source = schema.get_field("source")?;
    let field_path = schema.get_field("path")?;
    let field_updated_at = schema.get_field("updated_at")?;
    let field_id = schema.get_field("id")?;

    let mut index_writer = index.writer(50_000_000)?;

    sources
        .iter()
        .try_for_each(|(name, source)| -> anyhow::Result<()> {
            for entry in fs::read_dir(source)? {
                let path = entry?.path();
                if !path.is_index_target() {
                    continue;
                }

                match path.clone().to_str() {
                    None => {
                        warn!("Skip {:?}, path string contains non-UTF8 string", path);
                    }
                    Some(path_str) => {
                        let relative_path =
                            match source.to_str().and_then(|x| path_str.strip_prefix(x)) {
                                Some(s) => s.trim_start_matches(is_separator),
                                None => {
                                    warn!("Skip {path_str}, failed to get a relative path");
                                    continue;
                                }
                            };

                        // Delete an old document
                        let id = format!("{}:{}", name, relative_path);
                        index_writer.delete_term(Term::from_field_text(field_id, id.as_str()));

                        let body = fs::read_to_string(path)?;
                        if body.is_empty() {
                            continue;
                        }

                        // Treat the first line as the title of the Markdown file and remove all leading # characters.
                        let title = body.lines().nth(0).unwrap().trim_start_matches("#").trim();

                        let mut doc = TantivyDocument::default();
                        doc.add_text(field_title, title);
                        doc.add_text(field_body, body);
                        doc.add_text(field_source, name);
                        doc.add_text(field_path, relative_path);

                        let now =
                            tantivy::DateTime::from_timestamp_secs(chrono::Utc::now().timestamp());
                        doc.add_date(field_updated_at, now);

                        doc.add_text(field_id, id);

                        index_writer.add_document(doc)?;
                    }
                };
            }
            index_writer.commit()?;

            Ok(())
        })?;

    Ok(())
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

trait PathExt {
    fn is_hidden(&self) -> bool;

    fn is_regular_file(&self) -> bool;

    fn is_index_target(&self) -> bool;
}

impl PathExt for Path {
    fn is_hidden(&self) -> bool {
        self.file_name()
            .and_then(|f| f.to_str().map(|f| f.starts_with(".")))
            .unwrap_or_default()
    }

    fn is_regular_file(&self) -> bool {
        self.is_file() && !self.is_hidden()
    }

    fn is_index_target(&self) -> bool {
        if !self.is_regular_file() {
            return false;
        }
        match self.extension().and_then(OsStr::to_str) {
            Some("md") | Some("txt") => true,
            _ => false,
        }
    }
}
