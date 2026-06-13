//! Inbound webhook → mail bridge.
//!
//! Each `[webhooks.<name>]` exposes `/webhooks/<name>`, converting the request
//! body into mail on a topic (or to a recipient) via the `mail.send` path.
//! Wake-on-mail then wakes a subscribed standing agent with the payload as its
//! prompt.

use subtle::ConstantTimeEq;

use crate::shared::config::WebhookConfig;

/// Header carrying the per-webhook secret. Distinct from provider-native
/// headers so a reverse proxy can remap provider auth onto it unambiguously.
pub const WEBHOOK_TOKEN_HEADER: &str = "x-grimoire-webhook-token";

/// Webhook auth outcome, an enum (not `bool`) so the handler picks a status
/// code per failure mode.
#[derive(Debug, PartialEq, Eq)]
pub enum WebhookAuth {
    /// No `secret` configured; endpoint is open by operator choice.
    Open,
    Match,
    /// Secret required but not presented.
    Missing,
    /// Secret presented but wrong.
    Mismatch,
}

/// Constant-time check of the presented token against the config secret.
/// `secret = None` opening the endpoint is intentional: operators opt into auth.
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

/// Resolve the webhook config to a mail address (`topic://…` or `agent://…`),
/// or a mail-layer error code for the handler to surface.
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
