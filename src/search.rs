use std::collections::HashMap;
use std::path::PathBuf;

use chrono::Local;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::Value;
use tantivy::{ReloadPolicy, TantivyDocument};

#[derive(Debug)]
pub struct Doc {
    pub title: String,
    pub updated_at: chrono::DateTime<Local>,
    pub source: String,
    pub path: PathBuf,
}

impl Doc {
    pub fn absolute_path(&self, sources: &HashMap<String, PathBuf>) -> anyhow::Result<PathBuf> {
        sources
            .get(self.source.as_str())
            .map(|x| PathBuf::from(x).join(self.path.clone()))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Failed to get the absolute path from source '{}' and path '{}'.",
                    self.source,
                    self.path.to_string_lossy()
                )
            })
    }
}

pub fn search(
    index: &tantivy::index::Index,
    query: &str,
    limit: usize,
) -> anyhow::Result<Vec<Doc>> {
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()?;
    let searcher = reader.searcher();

    let schema = index.schema();
    let field_title = schema.get_field("title")?;
    let field_body = schema.get_field("body")?;
    let field_source = schema.get_field("source")?;
    let field_path = schema.get_field("path")?;
    let field_updated_at = schema.get_field("updated_at")?;

    let query_parser = {
        let mut query_parser =
            QueryParser::for_index(index, vec![field_title, field_body, field_updated_at]);
        query_parser.set_conjunction_by_default();
        query_parser
    };
    let query = query_parser.parse_query(query)?;

    let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

    top_docs
        .into_iter()
        .map(|(_, doc_address)| {
            searcher
                .doc(doc_address)
                .map(|doc: TantivyDocument| {
                    let title = doc
                        .get_first(field_title)
                        .and_then(|x| x.as_str().map(String::from))
                        .unwrap_or_default();
                    let path = doc
                        .get_first(field_path)
                        .and_then(|x| x.as_str().map(PathBuf::from))
                        .unwrap_or_default();
                    let updated_at = doc
                        .get_first(field_updated_at)
                        .and_then(|x| x.as_datetime())
                        .and_then(|t| {
                            chrono::DateTime::from_timestamp_secs(t.into_timestamp_secs())
                        })
                        .map(chrono::DateTime::<chrono::Local>::from)
                        .unwrap_or_default();
                    let source = doc
                        .get_first(field_source)
                        .and_then(|x| x.as_str().map(String::from))
                        .unwrap_or_default();

                    Doc {
                        title,
                        source,
                        path,
                        updated_at,
                    }
                })
                .map_err(anyhow::Error::new)
        })
        .collect()
}
