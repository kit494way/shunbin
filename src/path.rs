use std::ffi::OsStr;
use std::path::Path;

pub trait PathExt {
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
