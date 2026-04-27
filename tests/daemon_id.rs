//! Federation Task 1 contract tests for `daemon_id::load_or_mint`.

use grimoire::daemon::daemon_id::load_or_mint;
use grimoire::shared::types::validate_daemon_id;
use std::fs;

#[test]
fn load_or_mint_creates_file_when_absent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("daemon.id");
    let id = load_or_mint(&path).unwrap();
    assert!(validate_daemon_id(&id));
    let on_disk = fs::read_to_string(&path).unwrap().trim().to_string();
    assert_eq!(id, on_disk);
}

#[test]
fn load_or_mint_returns_existing_value_when_present() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("daemon.id");
    fs::write(&path, "abcd1234\n").unwrap();
    let mtime_before = fs::metadata(&path).unwrap().modified().unwrap();
    let id = load_or_mint(&path).unwrap();
    assert_eq!(id, "abcd1234");
    let mtime_after = fs::metadata(&path).unwrap().modified().unwrap();
    assert_eq!(mtime_before, mtime_after, "file must not be re-written");
}

#[test]
fn load_or_mint_rejects_invalid_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("daemon.id");
    fs::write(&path, "NOTHEX").unwrap();
    let err = load_or_mint(&path).unwrap_err();
    assert!(format!("{}", err).contains("invalid_daemon_id_file"));
}

#[test]
fn validate_daemon_id_accepts_lowercase_hex_only() {
    assert!(validate_daemon_id("abcd1234"));
    assert!(!validate_daemon_id("ABCD1234"));
    assert!(!validate_daemon_id("abcd123"));
    assert!(!validate_daemon_id("abcd1234x"));
    assert!(!validate_daemon_id(""));
}
