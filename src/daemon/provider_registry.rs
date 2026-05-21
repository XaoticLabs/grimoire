use std::collections::HashMap;
use std::sync::Arc;

use crate::shared::config::Config;

use super::provider::Provider;
use super::providers::claude::ClaudeProvider;
use super::providers::plain_text::PlainTextProvider;

pub struct ProviderRegistry {
    providers: HashMap<String, Arc<dyn Provider>>,
    default: String,
}

impl ProviderRegistry {
    pub fn from_config(config: &Config) -> Self {
        let mut providers: HashMap<String, Arc<dyn Provider>> = HashMap::new();

        // Always register the built-in Claude provider
        let claude_binary = config.claude_binary();
        providers.insert(
            "claude".to_string(),
            Arc::new(ClaudeProvider::new(claude_binary)),
        );

        // Register custom providers from config
        for (name, provider_config) in &config.providers {
            providers.insert(
                name.clone(),
                Arc::new(PlainTextProvider::new(
                    name.clone(),
                    provider_config.clone(),
                )),
            );
        }

        let default = config
            .agent
            .default_provider
            .clone()
            .unwrap_or_else(|| "claude".to_string());

        Self { providers, default }
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
                },
            )),
        );
        Self {
            providers,
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
            "codex".to_string(),
            ProviderConfig {
                binary: "codex".to_string(),
                args_template: vec!["-q".to_string(), "{task}".to_string()],
                env: HashMap::new(),
            },
        );

        let registry = ProviderRegistry::from_config(&config);
        assert!(registry.get("codex").is_some());
        assert!(registry.get("claude").is_some());
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
        config.agent.default_provider = Some("codex".to_string());
        config.providers.insert(
            "codex".to_string(),
            ProviderConfig {
                binary: "codex".to_string(),
                args_template: vec![],
                env: HashMap::new(),
            },
        );

        let registry = ProviderRegistry::from_config(&config);
        assert_eq!(registry.default_name(), "codex");
    }
}
