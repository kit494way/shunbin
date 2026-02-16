use std::collections::VecDeque;
use std::fs;
use std::io;
use std::path::PathBuf;

use crate::path::PathExt;

/// An iterator that recursively traverses a directory tree and yields paths to indexable files.
#[derive(Debug)]
pub struct RecursiveReadDir {
    it: fs::ReadDir,
    dirs: VecDeque<PathBuf>,
}

impl RecursiveReadDir {
    pub fn new(dir: PathBuf) -> io::Result<Self> {
        let it = fs::read_dir(dir)?;
        Ok(Self {
            it,
            dirs: VecDeque::<PathBuf>::new(),
        })
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
