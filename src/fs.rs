use std::collections::VecDeque;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use chrono::DateTime;
use chrono::Utc;

use crate::path::PathExt;

/// An iterator that recursively traverses a directory tree and yields paths to indexable files.
#[derive(Debug)]
pub struct RecursiveReadDir {
    it: fs::ReadDir,
    dirs: VecDeque<PathBuf>,
    last_updated_at: Option<SystemTime>,
}

impl RecursiveReadDir {
    pub fn new(dir: PathBuf) -> io::Result<Self> {
        let it = fs::read_dir(dir)?;
        Ok(Self {
            it,
            dirs: VecDeque::<PathBuf>::new(),
            last_updated_at: None,
        })
    }

    pub fn updated_after(mut self, datetime: DateTime<Utc>) -> Self {
        self.last_updated_at = Some(datetime.into());
        self
    }
}

impl Iterator for RecursiveReadDir {
    type Item = io::Result<PathBuf>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(x) = self.it.next() {
                let entry = match x {
                    Ok(p) => p,
                    Err(e) => return Some(Err(e)),
                };
                let path = entry.path();

                if path.is_hidden() {
                    continue;
                }

                if path.is_dir() {
                    self.dirs.push_back(path);
                    continue;
                }

                if !path.is_index_target() {
                    continue;
                }

                if let Some(time) = self.last_updated_at
                    && !is_updated_after(&path, time).unwrap_or(true)
                {
                    continue;
                }

                return Some(Ok(path));
            }

            let Some(dir) = self.dirs.pop_front() else {
                return None;
            };

            self.it = match fs::read_dir(dir) {
                Ok(x) => x,
                Err(e) => return Some(Err(e)),
            };
        }
    }
}

fn is_updated_after(path: &Path, time: SystemTime) -> io::Result<bool> {
    let meta = fs::metadata(path)?;
    Ok(meta.modified()? > time)
}
