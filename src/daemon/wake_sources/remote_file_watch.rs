//! Remote file-watch wake source: the shadow side of a federated workspace.
//! No local notify watcher — the registry matches paths off the
//! `WorkspaceFileChanged` events republished after a peer delivery.
//!
//! `workspace_id` is the **local shadow** id (the receiver's side, not the home
//! daemon's). `globs`/`ignore` match the reported rel-paths as sent; there's no
//! on-disk root to canonicalize against.

use anyhow::{Result, anyhow};
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteFileWatchConfig {
    pub workspace_id: String,
    pub globs: Vec<String>,
    #[serde(default)]
    pub ignore: Vec<String>,
}

pub struct RemoteFileWatchSource {
    pub config: RemoteFileWatchConfig,
    pub include_set: GlobSet,
    pub ignore_set: GlobSet,
}

impl RemoteFileWatchSource {
    pub fn new(config: RemoteFileWatchConfig) -> Result<Self> {
        if config.workspace_id.is_empty() {
            return Err(anyhow!("workspace_id_required"));
        }
        let include_set = build_globset(&config.globs)?;
        let ignore_set = build_globset(&config.ignore)?;
        Ok(Self {
            config,
            include_set,
            ignore_set,
        })
    }

    /// Test a rel-path (as published by the home daemon) against the
    /// configured globs. Ignores win over includes.
    pub fn matches(&self, rel_path: &str) -> bool {
        let p = Path::new(rel_path);
        if self.ignore_set.is_match(p) {
            return false;
        }
        self.include_set.is_match(p)
    }
}

fn build_globset(patterns: &[String]) -> Result<GlobSet> {
    let mut b = GlobSetBuilder::new();
    for p in patterns {
        let g = Glob::new(p).map_err(|e| anyhow!("invalid_glob: {e}"))?;
        b.add(g);
    }
    Ok(b.build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_workspace_id_rejected() {
        let cfg = RemoteFileWatchConfig {
            workspace_id: String::new(),
            globs: vec!["**/*.rs".into()],
            ignore: vec![],
        };
        assert!(RemoteFileWatchSource::new(cfg).is_err());
    }

    #[test]
    fn matches_includes_and_excludes_ignore() {
        let cfg = RemoteFileWatchConfig {
            workspace_id: "shadow-1".into(),
            globs: vec!["**/*.rs".into()],
            ignore: vec!["target/**".into()],
        };
        let s = RemoteFileWatchSource::new(cfg).unwrap();
        assert!(s.matches("src/main.rs"));
        assert!(!s.matches("target/build.rs"));
        assert!(!s.matches("README.md"));
    }
}
