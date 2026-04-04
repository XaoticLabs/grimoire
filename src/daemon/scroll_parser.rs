use anyhow::{Result, anyhow};

/// Parsed spec before insertion into DB
#[derive(Debug)]
pub struct ScrollSpec {
    pub name: String,
    pub runes: Vec<RuneSpec>,
}

#[derive(Debug)]
pub struct RuneSpec {
    pub name: String,
    pub task: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub cwd: Option<String>,
    pub file_patterns: Vec<String>,
    pub depends_on: Vec<String>, // Names, resolved to IDs during insertion
}

/// Parse a markdown spec file into a ScrollSpec.
///
/// Expected format:
/// ```markdown
/// # Scroll: My Project
///
/// ## Rune: Task Name
/// - files: src/foo.rs, src/bar.rs
/// - depends: Other Task, Another Task
/// - provider: claude
/// - model: sonnet
///
/// The actual task prompt goes here...
/// ```
pub fn parse_scroll(content: &str) -> Result<ScrollSpec> {
    let lines: Vec<&str> = content.lines().collect();

    // Extract scroll name from first # heading
    let name = lines
        .iter()
        .find(|l| l.starts_with("# "))
        .map(|l| {
            l.trim_start_matches("# ")
                .trim_start_matches("Scroll:")
                .trim_start_matches("Scroll: ")
                .trim()
                .to_string()
        })
        .ok_or_else(|| anyhow!("Spec must start with a '# Scroll: <name>' heading"))?;

    if name.is_empty() {
        return Err(anyhow!("Scroll name cannot be empty"));
    }

    // Split into rune sections on ## Rune: headings
    let mut runes = Vec::new();
    let mut current_rune: Option<(String, Vec<&str>)> = None;

    for line in &lines {
        if let Some(rune_name) = line
            .strip_prefix("## Rune:")
            .or_else(|| line.strip_prefix("## Rune: "))
        {
            // Save previous rune
            if let Some((name, body)) = current_rune.take() {
                runes.push(parse_rune_section(&name, &body)?);
            }
            current_rune = Some((rune_name.trim().to_string(), Vec::new()));
        } else if let Some((_, ref mut body)) = current_rune {
            body.push(line);
        }
    }

    // Save last rune
    if let Some((name, body)) = current_rune {
        runes.push(parse_rune_section(&name, &body)?);
    }

    if runes.is_empty() {
        return Err(anyhow!("Scroll must contain at least one '## Rune:' section"));
    }

    // Validate dependency references
    let rune_names: Vec<&str> = runes.iter().map(|r| r.name.as_str()).collect();
    for rune in &runes {
        for dep in &rune.depends_on {
            if !rune_names.contains(&dep.as_str()) {
                return Err(anyhow!(
                    "Rune '{}' depends on '{}' which doesn't exist. Available: {:?}",
                    rune.name,
                    dep,
                    rune_names
                ));
            }
        }
    }

    // Check for self-dependencies
    for rune in &runes {
        if rune.depends_on.contains(&rune.name) {
            return Err(anyhow!("Rune '{}' cannot depend on itself", rune.name));
        }
    }

    Ok(ScrollSpec { name, runes })
}

fn parse_rune_section(name: &str, body: &[&str]) -> Result<RuneSpec> {
    let mut provider = None;
    let mut model = None;
    let mut cwd = None;
    let mut file_patterns = Vec::new();
    let mut depends_on = Vec::new();
    let mut task_lines = Vec::new();
    let mut in_metadata = true;

    for line in body {
        let trimmed = line.trim();
        if trimmed.is_empty() && in_metadata {
            continue;
        }

        if in_metadata {
            if let Some(val) = trimmed.strip_prefix("- files:") {
                file_patterns = val.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                continue;
            }
            if let Some(val) = trimmed.strip_prefix("- depends:") {
                let val = val.trim();
                if !val.is_empty() && val != "(none)" && val != "none" {
                    depends_on = val.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                }
                continue;
            }
            if let Some(val) = trimmed.strip_prefix("- provider:") {
                provider = Some(val.trim().to_string());
                continue;
            }
            if let Some(val) = trimmed.strip_prefix("- model:") {
                model = Some(val.trim().to_string());
                continue;
            }
            if let Some(val) = trimmed.strip_prefix("- cwd:") {
                cwd = Some(val.trim().to_string());
                continue;
            }
            // If we hit a non-metadata line, switch to task collection
            if !trimmed.starts_with("- ") {
                in_metadata = false;
            }
        }

        if !in_metadata {
            task_lines.push(*line);
        }
    }

    let task = task_lines.join("\n").trim().to_string();
    if task.is_empty() {
        return Err(anyhow!(
            "Rune '{}' has no task text. Add task description after the metadata lines.",
            name
        ));
    }

    Ok(RuneSpec {
        name: name.to_string(),
        task,
        provider,
        model,
        cwd,
        file_patterns,
        depends_on,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_scroll() {
        let content = r#"# Scroll: Test Project

## Rune: Setup Database
- files: src/db.rs, migrations/
- depends: (none)

Create the database schema with users and sessions tables.

## Rune: API Routes
- files: src/routes.rs
- depends: Setup Database

Implement REST API endpoints for user management.

## Rune: Frontend
- provider: aider
- files: frontend/src/App.tsx
- depends: API Routes

Build the frontend login page.
"#;

        let spec = parse_scroll(content).unwrap();
        assert_eq!(spec.name, "Test Project");
        assert_eq!(spec.runes.len(), 3);

        assert_eq!(spec.runes[0].name, "Setup Database");
        assert_eq!(spec.runes[0].file_patterns, vec!["src/db.rs", "migrations/"]);
        assert!(spec.runes[0].depends_on.is_empty());

        assert_eq!(spec.runes[1].name, "API Routes");
        assert_eq!(spec.runes[1].depends_on, vec!["Setup Database"]);

        assert_eq!(spec.runes[2].name, "Frontend");
        assert_eq!(spec.runes[2].provider.as_deref(), Some("aider"));
        assert_eq!(spec.runes[2].depends_on, vec!["API Routes"]);
    }

    #[test]
    fn test_invalid_dependency() {
        let content = r#"# Scroll: Bad Deps

## Rune: Task A
- depends: Task C

Do something.
"#;

        let result = parse_scroll(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("doesn't exist"));
    }

    #[test]
    fn test_empty_scroll() {
        let content = "# Scroll: Empty\n";
        let result = parse_scroll(content);
        assert!(result.is_err());
    }

    #[test]
    fn test_all_metadata() {
        let content = r#"# Scroll: Full Meta

## Rune: Task A
- files: src/a.rs, src/b.rs
- depends: (none)
- provider: codex
- model: gpt-4
- cwd: /home/user

Do task A with all metadata.
"#;
        let spec = parse_scroll(content).unwrap();
        let rune = &spec.runes[0];
        assert_eq!(rune.provider.as_deref(), Some("codex"));
        assert_eq!(rune.model.as_deref(), Some("gpt-4"));
        assert_eq!(rune.cwd.as_deref(), Some("/home/user"));
        assert_eq!(rune.file_patterns, vec!["src/a.rs", "src/b.rs"]);
    }

    #[test]
    fn test_multi_dependency() {
        let content = r#"# Scroll: Multi Dep

## Rune: A

Do A.

## Rune: B

Do B.

## Rune: C
- depends: A, B

Do C after both A and B.
"#;
        let spec = parse_scroll(content).unwrap();
        assert_eq!(spec.runes[2].depends_on, vec!["A", "B"]);
    }

    #[test]
    fn test_self_dependency() {
        let content = r#"# Scroll: Self Dep

## Rune: A
- depends: A

Do A.
"#;
        let result = parse_scroll(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot depend on itself"));
    }

    #[test]
    fn test_no_heading() {
        let content = "Just some text without a heading\n";
        assert!(parse_scroll(content).is_err());
    }

    #[test]
    fn test_duplicate_rune_names() {
        let content = r#"# Scroll: Dup Names

## Rune: Task A

Do A.

## Rune: Task A

Do A again.
"#;
        // Duplicate names are allowed by the parser (distinct rune IDs assigned later)
        let spec = parse_scroll(content).unwrap();
        assert_eq!(spec.runes.len(), 2);
    }

    #[test]
    fn test_empty_scroll_name() {
        let content = "# Scroll:\n\n## Rune: A\n\nDo A.\n";
        let result = parse_scroll(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn test_rune_no_task_text() {
        let content = r#"# Scroll: No Task

## Rune: Empty
- files: src/foo.rs
"#;
        let result = parse_scroll(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no task text"));
    }
}
