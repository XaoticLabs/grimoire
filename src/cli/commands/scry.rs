use anyhow::Result;
use colored::Colorize;

use crate::shared::auth;
use crate::shared::constants;

pub async fn run() -> Result<()> {
    // Mint a one-shot login URL (`?t=…`) so the browser auto-authenticates;
    // fall back to the bare URL for manual token entry if the token won't load.
    let base = format!("http://127.0.0.1:{}", constants::DAEMON_PORT);
    let (url, authed) = match auth::load_for_client() {
        Ok(tok) => (format!("{}/auth/login?t={}", base, tok.as_str()), true),
        Err(_) => (base.clone(), false),
    };

    if authed {
        println!("🔮 Opening grimoire dashboard at {}", base.bold());
    } else {
        println!(
            "🔮 Opening grimoire dashboard at {} (sign in with your token from {})",
            base.bold(),
            auth::token_path().display(),
        );
    }

    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("open").arg(&url).spawn();
    }

    #[cfg(target_os = "linux")]
    {
        let _ = std::process::Command::new("xdg-open").arg(&url).spawn();
    }

    let _ = url; // silence unused on other targets
    Ok(())
}
