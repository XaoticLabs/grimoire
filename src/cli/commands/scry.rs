use anyhow::Result;
use colored::Colorize;

use crate::shared::auth;
use crate::shared::constants;

pub async fn run() -> Result<()> {
    // Mint a one-shot login URL so the browser is auto-authenticated. The
    // daemon validates `?t=…`, sets the `grim_auth` cookie, and redirects
    // to `/`. If the token isn't loadable we fall back to the bare URL,
    // and the user can paste the token into the login form.
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

    // Try to open browser
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
