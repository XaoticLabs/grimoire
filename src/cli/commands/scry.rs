use anyhow::Result;
use colored::Colorize;

use crate::shared::constants;

pub async fn run() -> Result<()> {
    let url = format!("http://127.0.0.1:{}", constants::DAEMON_PORT);
    println!("🔮 Opening grimoire dashboard at {}", url.bold());

    // Try to open browser
    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("open").arg(&url).spawn();
    }

    #[cfg(target_os = "linux")]
    {
        let _ = std::process::Command::new("xdg-open").arg(&url).spawn();
    }

    Ok(())
}
