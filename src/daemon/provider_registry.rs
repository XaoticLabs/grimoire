use std::collections::HashMap;
use std::sync::Arc;

use crate::shared::config::Config;

use super::provider::Provider;
use super::providers::claude::ClaudeProvider;
use super::providers::pi::PiProvider;
use super::providers::plain_text::PlainTextProvider;

pub struct ProviderRegistry {
    providers: HashMap<String, Arc<dyn Provider>>,
    sandboxes: HashMap<String, crate::shared::config::SandboxConfig>,
    default: String,
}

impl ProviderRegistry {
    pub fn from_config(config: &Config) -> Self {
        // Reserved built-in adapter names; `[providers.*]` may not shadow these.
        const RESERVED: [&str; 2] = ["claude", "pi"];

        let mut providers: HashMap<String, Arc<dyn Provider>> = HashMap::new();
        let mut sandboxes: HashMap<String, crate::shared::config::SandboxConfig> = HashMap::new();

        // `[providers.claude.sandbox]` / `[providers.pi.sandbox]` may configure
        // the built-in adapters even though those aren't built from `ProviderConfig`.
        for name in RESERVED {
            if let Some(pc) = config.providers.get(name)
                && let Some(sb) = &pc.sandbox
            {
                sandboxes.insert(name.to_string(), sb.clone());
            }
        }

        // Always register the built-in native-resume adapters. Their names
        // are reserved: a `[providers.*]` config entry may not shadow them
        // (doing so would swap a native-session adapter for the generic
        // transcript-replay one and silently lose resume fidelity).
        providers.insert(
            "claude".to_string(),
            Arc::new(ClaudeProvider::new(config.claude_binary())),
        );
        providers.insert(
            "pi".to_string(),
            Arc::new(PiProvider::new(config.pi_binary())),
        );

        // Register custom providers from config via the generic adapter.
        for (name, provider_config) in &config.providers {
            if RESERVED.contains(&name.as_str()) {
                tracing::warn!(
                    provider = %name,
                    "ignoring [providers.{name}]: '{name}' is a reserved built-in adapter; \
                     set agent.{name}_binary to override its binary path instead"
                );
                continue;
            }
            providers.insert(
                name.clone(),
                Arc::new(PlainTextProvider::new(
                    name.clone(),
                    provider_config.clone(),
                )),
            );
            if let Some(sb) = &provider_config.sandbox {
                sandboxes.insert(name.clone(), sb.clone());
            }
        }

        let default = config
            .agent
            .default_provider
            .clone()
            .unwrap_or_else(|| "claude".to_string());

        Self {
            providers,
            sandboxes,
            default,
        }
    }

    /// Per-provider [`SandboxConfig`] resolved from `[providers.<name>.sandbox]`.
    /// Returns `None` when the provider isn't sandbox-configured, leaving the
    /// agent unconfined (current default).
    pub fn sandbox_for(&self, name: &str) -> Option<crate::shared::config::SandboxConfig> {
        self.sandboxes.get(name).cloned()
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Provider>> {
        self.providers.get(name).cloned()
    }

    pub fn default_provider(&self) -> Arc<dyn Provider> {
        self.providers
            .get(&self.default)
            .cloned()
            .unwrap_or_else(|| {
                self.providers
                    .get("claude")
                    .cloned()
                    .expect("'claude' provider is always registered")
            })
    }

    pub fn default_name(&self) -> &str {
        &self.default
    }

    pub fn list(&self) -> Vec<String> {
        self.providers.keys().cloned().collect()
    }

    /// Test helper: registry with a single provider named `true_provider` that
    /// spawns `/bin/true`.
    pub fn test_with_true_provider() -> Self {
        let mut providers: HashMap<String, Arc<dyn Provider>> = HashMap::new();
        providers.insert(
            "true_provider".to_string(),
            Arc::new(PlainTextProvider::new(
                "true_provider".to_string(),
                crate::shared::config::ProviderConfig {
                    binary: "true".to_string(),
                    args_template: vec![],
                    env: HashMap::new(),
                sandbox: None,
                pricing: None,
                },
            )),
        );
        Self {
            providers,
            sandboxes: HashMap::new(),
            default: "true_provider".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::config::{Config, ProviderConfig};
    use std::collections::HashMap;

    #[test]
    fn default_has_claude() {
        let config = Config::default();
        let registry = ProviderRegistry::from_config(&config);
        assert!(registry.get("claude").is_some());
        assert_eq!(registry.default_name(), "claude");
    }

    #[test]
    fn custom_provider_from_config() {
        let mut config = Config::default();
        config.providers.insert(
            "aider".to_string(),
            ProviderConfig {
                binary: "aider".to_string(),
                args_template: vec!["--message".to_string(), "{task}".to_string()],
                env: HashMap::new(),
                sandbox: None,
                pricing: None,
            },
        );

        let registry = ProviderRegistry::from_config(&config);
        assert!(registry.get("aider").is_some());
        assert!(registry.get("claude").is_some());
    }

    #[test]
    fn pi_builtin_registered() {
        let registry = ProviderRegistry::from_config(&Config::default());
        assert!(registry.get("pi").is_some());
        // Built-in native adapter resumes by the CLI's own session.
        assert!(registry.get("pi").unwrap().capabilities().supports_resume);
    }

    #[test]
    fn reserved_name_config_entry_is_ignored() {
        let mut config = Config::default();
        // A user trying to redefine `pi` as a generic args_template provider
        // must not clobber the native built-in (which would lose resume).
        config.providers.insert(
            "pi".to_string(),
            ProviderConfig {
                binary: "pi".to_string(),
                args_template: vec!["{task}".to_string()],
                env: HashMap::new(),
                sandbox: None,
                pricing: None,
            },
        );

        let registry = ProviderRegistry::from_config(&config);
        // Still the native adapter, not the generic PlainText one.
        assert!(registry.get("pi").unwrap().capabilities().supports_resume);
    }

    #[test]
    fn unknown_provider_returns_none() {
        let config = Config::default();
        let registry = ProviderRegistry::from_config(&config);
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn custom_default_provider() {
        let mut config = Config::default();
        config.agent.default_provider = Some("aider".to_string());
        config.providers.insert(
            "aider".to_string(),
            ProviderConfig {
                binary: "aider".to_string(),
                args_template: vec![],
                env: HashMap::new(),
                sandbox: None,
                pricing: None,
            },
        );

        let registry = ProviderRegistry::from_config(&config);
        assert_eq!(registry.default_name(), "aider");
    }
}
