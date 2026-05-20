//! File-watch wake source: arms a `notify::RecommendedWatcher` over the
//! agent's cwd and fires (debounced) when a path under it matches the
//! configured glob set and is not in the ignore set.
//!
//! The bridge from `notify`'s sync callback into the registry's tokio
//! runtime is a bounded `tokio::sync::mpsc::Sender` — the watcher thread
//! pushes events, a debounce task drains them.

use anyhow::{Result, anyhow};
use globset::{Glob, GlobSet, GlobSetBuilder};
use notify::{Event as NotifyEvent, EventKind, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const DEBOUNCE_MS: u64 = 200;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileWatchConfig {
    pub globs: Vec<String>,
    #[serde(default)]
    pub ignore: Vec<String>,
    pub root: PathBuf,
}

pub struct FileWatchSource {
    pub config: FileWatchConfig,
    pub root_canonical: PathBuf,
    pub include_set: GlobSet,
    pub ignore_set: GlobSet,
}

impl FileWatchSource {
    pub fn new(config: FileWatchConfig) -> Result<Self> {
        if !config.root.exists() || !config.root.is_dir() {
            return Err(anyhow!("cwd_gone"));
        }
        let root_canonical =
            std::fs::canonicalize(&config.root).map_err(|_| anyhow!("cwd_gone"))?;

        let include_set = build_globset(&config.globs)?;
        let ignore_set = build_globset(&config.ignore)?;
        Ok(Self {
            config,
            root_canonical,
            include_set,
            ignore_set,
        })
    }

    pub fn matches(&self, path: &Path) -> bool {
        // Strip root prefix if present so globs are interpreted relative to
        // the watched root.
        let Ok(relative) = path.strip_prefix(&self.root_canonical) else {
            return false;
        };
        if self.ignore_set.is_match(relative) {
            return false;
        }
        self.include_set.is_match(relative)
    }

    /// Spawn a `notify` watcher (blocking, on its own thread) that pushes
    /// `MatchedChange` events through the supplied tokio Sender. Returns
    /// the watcher; dropping it stops the watch.
    pub fn arm(
        self: Arc<Self>,
        tx: tokio::sync::mpsc::Sender<MatchedChange>,
    ) -> Result<notify::RecommendedWatcher> {
        let me = self.clone();
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<NotifyEvent>| {
            let event = match res {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(error = %e, "notify error");
                    return;
                }
            };
            if !matches!(
                event.kind,
                EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
            ) {
                return;
            }
            for path in event.paths {
                if me.matches(&path) {
                    let _ = tx.try_send(MatchedChange { path });
                }
            }
        })?;
        watcher.watch(&self.root_canonical, RecursiveMode::Recursive)?;
        Ok(watcher)
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

pub struct MatchedChange {
    pub path: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn missing_root_fails_with_cwd_gone() {
        let cfg = FileWatchConfig {
            globs: vec!["*.rs".into()],
            ignore: vec![],
            root: PathBuf::from("/no/such/path/here/zzz"),
        };
        match FileWatchSource::new(cfg) {
            Err(e) => assert_eq!(e.to_string(), "cwd_gone"),
            Ok(_) => panic!("expected cwd_gone"),
        }
    }

    #[test]
    fn matches_includes_and_excludes_ignore() {
        let dir = tempdir().unwrap();
        let cfg = FileWatchConfig {
            globs: vec!["**/*.rs".into()],
            ignore: vec!["target/**".into()],
            root: dir.path().to_path_buf(),
        };
        let s = FileWatchSource::new(cfg).unwrap();
        assert!(s.matches(&dir.path().canonicalize().unwrap().join("src/main.rs")));
        assert!(!s.matches(&dir.path().canonicalize().unwrap().join("target/build.rs")));
    }
}
