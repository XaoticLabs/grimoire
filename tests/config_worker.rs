// RED tests for worker-pool spec, Task 9: config additions for daemon
// `[worker]` block + `grimw.toml`.
//
// References `WorkerConfig` field on `DaemonConfig` and a future `GrimwConfig`
// in the `grimw` module.

use std::sync::atomic::Ordering;

use grimoire::shared::config::{Config, WorkerConfig};

#[test]
fn daemon_config_parses_with_worker_block() {
    let toml = r#"
[daemon]
port = 7777

[worker]
listen_addr = "127.0.0.1:7878"
secret = "shh"
heartbeat_timeout_secs = 30
heartbeat_interval_hint_secs = 5
tls_cert_path = "/tmp/cert.pem"
tls_key_path = "/tmp/key.pem"
"#;
    let cfg: Config = toml::from_str(toml).expect("config parses");
    let worker: &WorkerConfig = cfg
        .worker
        .as_ref()
        .expect("worker block parsed into Some");
    assert_eq!(worker.secret, "shh");
    assert_eq!(worker.listen_addr.to_string(), "127.0.0.1:7878");
    assert_eq!(worker.heartbeat_timeout_secs, 30);
}

#[test]
fn daemon_config_parses_without_worker_block() {
    let toml = r#"
[daemon]
port = 7777
"#;
    let cfg: Config = toml::from_str(toml).expect("legacy config parses");
    assert!(
        cfg.worker.is_none(),
        "absent [worker] block must deserialize to None"
    );
}

#[test]
fn daemon_config_rejects_worker_without_secret() {
    let toml = r#"
[worker]
listen_addr = "127.0.0.1:7878"
"#;
    let err = toml::from_str::<Config>(toml).expect_err("missing secret rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("secret"),
        "error must mention `secret`; got: {msg}"
    );
}

#[test]
fn daemon_config_listen_addr_defaults_to_loopback() {
    let toml = r#"
[worker]
secret = "shh"
"#;
    let cfg: Config = toml::from_str(toml).expect("config parses with default listen_addr");
    let worker = cfg.worker.as_ref().unwrap();
    assert_eq!(
        worker.listen_addr.to_string(),
        "127.0.0.1:7878",
        "listen_addr must default to loopback, never 0.0.0.0"
    );
}

// RED tests for durable-work-queue spec, Task 6: `daemon.max_concurrent_agents`
// configuration with `serde(default)` of 8 and an `Arc<AtomicU32>` reload path.

#[test]
fn daemon_config_max_concurrent_default_is_eight() {
    let toml = r#"
[daemon]
port = 7777
"#;
    let cfg: Config = toml::from_str(toml).expect("config parses without max_concurrent_agents");
    assert_eq!(
        cfg.daemon.max_concurrent_agents, 8,
        "absent max_concurrent_agents must default to 8"
    );
}

#[test]
fn daemon_config_max_concurrent_default_when_daemon_block_missing() {
    let toml = "";
    let cfg: Config = toml::from_str(toml).expect("empty config parses");
    assert_eq!(cfg.daemon.max_concurrent_agents, 8);
}

#[test]
fn daemon_config_max_concurrent_explicit() {
    let toml = r#"
[daemon]
max_concurrent_agents = 16
"#;
    let cfg: Config = toml::from_str(toml).expect("explicit value parses");
    assert_eq!(cfg.daemon.max_concurrent_agents, 16);
}

#[test]
fn daemon_config_max_concurrent_zero_is_valid() {
    let toml = r#"
[daemon]
max_concurrent_agents = 0
"#;
    let cfg: Config = toml::from_str(toml).expect("zero is a valid operator override");
    assert_eq!(cfg.daemon.max_concurrent_agents, 0);
}

#[test]
fn daemon_config_max_concurrent_atomic_reflects_reload() {
    let initial: Config = toml::from_str(
        r#"
[daemon]
max_concurrent_agents = 4
"#,
    )
    .unwrap();
    let atomic = initial.max_concurrent_atomic();
    let shared = atomic.clone();
    assert_eq!(shared.load(Ordering::Relaxed), 4);

    let reloaded: Config = toml::from_str(
        r#"
[daemon]
max_concurrent_agents = 12
"#,
    )
    .unwrap();
    reloaded.apply_max_concurrent(&atomic);

    assert_eq!(
        shared.load(Ordering::Relaxed),
        12,
        "shared Arc<AtomicU32> must reflect post-reload value"
    );
}

#[test]
fn grimw_config_parses_minimum_example() {
    use grimoire::grimw::config::GrimwConfig;

    let toml = r#"
daemon_url = "https://daemon.tailnet.ts.net:7878"
secret = "shared-bearer-token-here"
daemon_cert_sha256 = "abcd1234"
max_concurrent = 4
tags = ["beefy"]

[providers.claude]
binary = "/usr/local/bin/claude"
"#;
    let cfg: GrimwConfig = toml::from_str(toml).expect("grimw.toml parses");
    assert_eq!(cfg.daemon_url, "https://daemon.tailnet.ts.net:7878");
    assert_eq!(cfg.secret, "shared-bearer-token-here");
    assert_eq!(cfg.daemon_cert_sha256, "abcd1234");
    assert_eq!(cfg.max_concurrent, 4);
    assert_eq!(cfg.tags, vec!["beefy".to_string()]);
    assert!(cfg.providers.contains_key("claude"));
    assert_eq!(cfg.providers["claude"].binary, "/usr/local/bin/claude");
}
