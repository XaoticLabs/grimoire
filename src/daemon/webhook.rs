//! Inbound webhook → mail bridge.
//!
//! Each configured `[webhooks.<name>]` exposes one HTTP endpoint at
//! `/webhooks/<name>` that converts the raw request body into mail on a
//! configured topic (or to a direct recipient), then drops it into the
//! existing `mail.send` path. Wake-on-mail picks it up from there, so a
//! standing agent subscribed to the topic gets woken with the webhook
//! payload as its prompt, no extra wiring required.
//!
//! This is the missing leg that turns the standing-agent demo from
//! "watch files locally" into "watch your real GitHub PRs": a CI / GitHub /
//! Slack / Linear hook posts here, an agent wakes, acts, sleeps.

use subtle::ConstantTimeEq;

use crate::shared::config::WebhookConfig;

/// HTTP header callers present the per-webhook shared secret in. Picked to
/// be specific enough not to collide with provider-native headers (GitHub's
/// `X-Hub-Signature-256`, Slack's `X-Slack-Signature`), so a reverse proxy
/// can map provider auth → this header without ambiguity.
pub const WEBHOOK_TOKEN_HEADER: &str = "x-grimoire-webhook-token";

/// Outcome of authenticating a webhook request against its config. Modeled
/// as an enum (not just `bool`) so the HTTP handler can pick the correct
/// status code per failure mode without re-parsing the config.
#[derive(Debug, PartialEq, Eq)]
pub enum WebhookAuth {
    /// The config has no `secret`; the endpoint is open. Operator's choice.
    Open,
    /// Secret matched (constant-time compared).
    Match,
    /// Secret required but not presented.
    Missing,
    /// Secret required and presented but did not match.
    Mismatch,
}

/// Constant-time check of the presented `X-Grimoire-Webhook-Token` against
/// the config secret. The early-out for `secret = None` is intentional and
/// part of the spec, operators opt in to auth, not the other way around.
pub fn check_token(presented: Option<&str>, expected: Option<&str>) -> WebhookAuth {
    match expected {
        None => WebhookAuth::Open,
        Some(want) => match presented {
            None => WebhookAuth::Missing,
            Some(got) => {
                if got.as_bytes().ct_eq(want.as_bytes()).into() {
                    WebhookAuth::Match
                } else {
                    WebhookAuth::Mismatch
                }
            }
        },
    }
}

/// Validate the webhook config and return the resolved mail address
/// (`topic://…` or `agent://…`). Returns the error code the HTTP handler
/// should surface in the response body, matching the mail-layer codes so
/// operators see one consistent vocabulary.
pub fn resolve_target(cfg: &WebhookConfig) -> Result<String, &'static str> {
    match (&cfg.topic, &cfg.recipient) {
        (Some(t), None) => Ok(format!("topic://{t}")),
        (None, Some(r)) => Ok(format!("agent://{r}")),
        (Some(_), Some(_)) => Err("webhook_config_ambiguous"),
        (None, None) => Err("webhook_config_missing_target"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(topic: Option<&str>, recipient: Option<&str>, secret: Option<&str>) -> WebhookConfig {
        WebhookConfig {
            topic: topic.map(str::to_string),
            recipient: recipient.map(str::to_string),
            secret: secret.map(str::to_string),
        }
    }

    #[test]
    fn open_when_no_secret_configured() {
        assert_eq!(check_token(None, None), WebhookAuth::Open);
        assert_eq!(check_token(Some("anything"), None), WebhookAuth::Open);
    }

    #[test]
    fn matches_correct_secret() {
        assert_eq!(check_token(Some("shhh"), Some("shhh")), WebhookAuth::Match);
    }

    #[test]
    fn rejects_missing_and_wrong_secrets() {
        assert_eq!(check_token(None, Some("shhh")), WebhookAuth::Missing);
        assert_eq!(
            check_token(Some("nope"), Some("shhh")),
            WebhookAuth::Mismatch
        );
    }

    #[test]
    fn resolve_target_topic_form() {
        assert_eq!(
            resolve_target(&cfg(Some("pr-opened"), None, None)).unwrap(),
            "topic://pr-opened"
        );
    }

    #[test]
    fn resolve_target_recipient_form() {
        assert_eq!(
            resolve_target(&cfg(None, Some("4a8c1b2f"), None)).unwrap(),
            "agent://4a8c1b2f"
        );
    }

    #[test]
    fn resolve_target_rejects_ambiguous_or_empty() {
        assert_eq!(
            resolve_target(&cfg(Some("a"), Some("b"), None)),
            Err("webhook_config_ambiguous")
        );
        assert_eq!(
            resolve_target(&cfg(None, None, None)),
            Err("webhook_config_missing_target")
        );
    }
}
