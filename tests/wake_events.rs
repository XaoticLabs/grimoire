//! Contract tests for the four new `StreamEvent` wake variants (T2).

use grimoire::shared::protocol::StreamEvent;

fn rt(ev: &StreamEvent) -> StreamEvent {
    let v = serde_json::to_value(ev).unwrap();
    serde_json::from_value(v).unwrap()
}

#[test]
fn wake_source_registered_serde_roundtrip() {
    let ev = StreamEvent::WakeSourceRegistered {
        wake_id: "wake_a1".into(),
        agent_id: "agent01".into(),
        kind: "cron".into(),
    };
    match rt(&ev) {
        StreamEvent::WakeSourceRegistered {
            wake_id,
            agent_id,
            kind,
        } => {
            assert_eq!(wake_id, "wake_a1");
            assert_eq!(agent_id, "agent01");
            assert_eq!(kind, "cron");
        }
        _ => panic!("wrong variant"),
    }
    assert_eq!(ev.kind(), "wake_source_registered");
}

#[test]
fn wake_source_fired_with_via_test() {
    let ev = StreamEvent::WakeSourceFired {
        wake_id: "wake_a1".into(),
        agent_id: "agent01".into(),
        mail_id: "mail0001".into(),
        via: Some("test".into()),
    };
    match rt(&ev) {
        StreamEvent::WakeSourceFired {
            wake_id,
            agent_id,
            mail_id,
            via,
        } => {
            assert_eq!(wake_id, "wake_a1");
            assert_eq!(agent_id, "agent01");
            assert_eq!(mail_id, "mail0001");
            assert_eq!(via.as_deref(), Some("test"));
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn wake_source_fired_via_none_roundtrip() {
    let ev = StreamEvent::WakeSourceFired {
        wake_id: "wake_a2".into(),
        agent_id: "agent02".into(),
        mail_id: "mail0002".into(),
        via: None,
    };
    match rt(&ev) {
        StreamEvent::WakeSourceFired { via, .. } => assert!(via.is_none()),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn wake_source_failed_with_reason() {
    let ev = StreamEvent::WakeSourceFailed {
        wake_id: "wake_a3".into(),
        agent_id: "agent03".into(),
        reason: "rate_limited".into(),
    };
    match rt(&ev) {
        StreamEvent::WakeSourceFailed { reason, .. } => {
            assert_eq!(reason, "rate_limited");
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn wake_source_retired_with_reason() {
    let ev = StreamEvent::WakeSourceRetired {
        wake_id: "wake_a4".into(),
        agent_id: "agent04".into(),
        reason: "agent_banished".into(),
    };
    match rt(&ev) {
        StreamEvent::WakeSourceRetired { reason, .. } => {
            assert_eq!(reason, "agent_banished");
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn wake_event_kinds_match_serde_renames() {
    let cases: &[(&str, StreamEvent)] = &[
        (
            "wake_source_registered",
            StreamEvent::WakeSourceRegistered {
                wake_id: "x".into(),
                agent_id: "a".into(),
                kind: "cron".into(),
            },
        ),
        (
            "wake_source_fired",
            StreamEvent::WakeSourceFired {
                wake_id: "x".into(),
                agent_id: "a".into(),
                mail_id: "m".into(),
                via: None,
            },
        ),
        (
            "wake_source_failed",
            StreamEvent::WakeSourceFailed {
                wake_id: "x".into(),
                agent_id: "a".into(),
                reason: "r".into(),
            },
        ),
        (
            "wake_source_retired",
            StreamEvent::WakeSourceRetired {
                wake_id: "x".into(),
                agent_id: "a".into(),
                reason: "r".into(),
            },
        ),
    ];
    for (expected, ev) in cases {
        assert_eq!(ev.kind(), *expected);
    }
}
