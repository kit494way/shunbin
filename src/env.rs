use std::env::{home_dir, var_os};
use std::path::PathBuf;

#[cfg(unix)]
pub fn xdg_config_home() -> anyhow::Result<PathBuf> {
    match var_os("XDG_CONFIG_HOME").map(PathBuf::from) {
        Some(x) => Ok(x),
        None => home_dir()
            .map(|x| x.join(".config"))
            .ok_or_else(|| anyhow::anyhow!("Failed to detect config file.")),
    }
}

#[cfg(windows)]
pub fn xdg_config_home() -> anyhow::Result<PathBuf> {
    match var_os("XDG_CONFIG_HOME").map(PathBuf::from) {
        Some(x) => Ok(x),
        None => var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("Failed to detect config file.")),
    }
}

#[cfg(unix)]
pub fn xdg_data_home() -> anyhow::Result<PathBuf> {
    match var_os("XDG_DATA_HOME").map(PathBuf::from) {
        Some(x) => Ok(x),
        None => home_dir()
            .map(|x| x.join(".local").join("share"))
            .ok_or_else(|| anyhow::anyhow!("Failed to detect config file.")),
    }
}

#[cfg(windows)]
pub fn xdg_data_home() -> anyhow::Result<PathBuf> {
    match var_os("XDG_DATA_HOME").map(PathBuf::from) {
        Some(x) => Ok(x),
        None => var_os("APPDATA")
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("Failed to detect config file.")),
    }
}
