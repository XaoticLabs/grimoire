use anyhow::{Result, anyhow};

/// Parsed spec before insertion into DB
#[derive(Debug)]
pub struct ScrollSpec {
    pub name: String,
    pub tasks: Vec<TaskSpec>,
    pub workspace: Option<String>,
    pub workspace_repo: Option<String>,
    pub workspace_branch: Option<String>,
}

#[derive(Debug)]
pub struct TaskSpec {
    pub name: String,
    pub prompt: String,
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
/// ## Task: Task Name
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

    // Split into task sections on ## Task: headings
    let mut tasks = Vec::new();
    let mut current_task: Option<(String, Vec<&str>)> = None;
    let mut workspace: Option<String> = None;
    let mut workspace_repo: Option<String> = None;
    let mut workspace_branch: Option<String> = None;

    for line in &lines {
        // Top-level workspace directives (between scroll heading and first task).
        if current_task.is_none() {
            let trimmed = line.trim();
            if let Some(v) = trimmed.strip_prefix("- workspace_repo:") {
                workspace_repo = Some(v.trim().to_string());
                continue;
            }
            if let Some(v) = trimmed.strip_prefix("- workspace_branch:") {
                workspace_branch = Some(v.trim().to_string());
                continue;
            }
            if let Some(v) = trimmed.strip_prefix("- workspace:") {
                workspace = Some(v.trim().to_string());
                continue;
            }
        }
        if let Some(task_name) = line
            .strip_prefix("## Task:")
            .or_else(|| line.strip_prefix("## Task: "))
        {
            // Save previous task
            if let Some((name, body)) = current_task.take() {
                tasks.push(parse_task_section(&name, &body)?);
            }
            current_task = Some((task_name.trim().to_string(), Vec::new()));
        } else if let Some((_, ref mut body)) = current_task {
            body.push(line);
        }
    }

    // Save last task
    if let Some((name, body)) = current_task {
        tasks.push(parse_task_section(&name, &body)?);
    }

    if tasks.is_empty() {
        return Err(anyhow!(
            "Scroll must contain at least one '## Task:' section"
        ));
    }

    // Validate dependency references
    let task_names: Vec<&str> = tasks.iter().map(|r| r.name.as_str()).collect();
    for task in &tasks {
        for dep in &task.depends_on {
            if !task_names.contains(&dep.as_str()) {
                return Err(anyhow!(
                    "Task '{}' depends on '{}' which doesn't exist. Available: {:?}",
                    task.name,
                    dep,
                    task_names
                ));
            }
        }
    }

    // Check for self-dependencies
    for task in &tasks {
        if task.depends_on.contains(&task.name) {
            return Err(anyhow!("Task '{}' cannot depend on itself", task.name));
        }
    }

    Ok(ScrollSpec {
        name,
        tasks,
        workspace,
        workspace_repo,
        workspace_branch,
    })
}

fn parse_task_section(name: &str, body: &[&str]) -> Result<TaskSpec> {
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
                file_patterns = val
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                continue;
            }
            if let Some(val) = trimmed.strip_prefix("- depends:") {
                let val = val.trim();
                if !val.is_empty() && val != "(none)" && val != "none" {
                    depends_on = val
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
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

    let prompt = task_lines.join("\n").trim().to_string();
    if prompt.is_empty() {
        return Err(anyhow!(
            "Task '{}' has no task text. Add task description after the metadata lines.",
            name
        ));
    }

    Ok(TaskSpec {
        name: name.to_string(),
        prompt,
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

## Task: Setup Database
- files: src/db.rs, migrations/
- depends: (none)

Create the database schema with users and sessions tables.

## Task: API Routes
- files: src/routes.rs
- depends: Setup Database

Implement REST API endpoints for user management.

## Task: Frontend
- provider: aider
- files: frontend/src/App.tsx
- depends: API Routes

Build the frontend login page.
"#;

        let spec = parse_scroll(content).unwrap();
        assert_eq!(spec.name, "Test Project");
        assert_eq!(spec.tasks.len(), 3);

        assert_eq!(spec.tasks[0].name, "Setup Database");
        assert_eq!(
            spec.tasks[0].file_patterns,
            vec!["src/db.rs", "migrations/"]
        );
        assert!(spec.tasks[0].depends_on.is_empty());

        assert_eq!(spec.tasks[1].name, "API Routes");
        assert_eq!(spec.tasks[1].depends_on, vec!["Setup Database"]);

        assert_eq!(spec.tasks[2].name, "Frontend");
        assert_eq!(spec.tasks[2].provider.as_deref(), Some("aider"));
        assert_eq!(spec.tasks[2].depends_on, vec!["API Routes"]);
    }

    #[test]
    fn test_invalid_dependency() {
        let content = r#"# Scroll: Bad Deps

## Task: Task A
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

## Task: Task A
- files: src/a.rs, src/b.rs
- depends: (none)
- provider: codex
- model: gpt-4
- cwd: /home/user

Do task A with all metadata.
"#;
        let spec = parse_scroll(content).unwrap();
        let task = &spec.tasks[0];
        assert_eq!(task.provider.as_deref(), Some("codex"));
        assert_eq!(task.model.as_deref(), Some("gpt-4"));
        assert_eq!(task.cwd.as_deref(), Some("/home/user"));
        assert_eq!(task.file_patterns, vec!["src/a.rs", "src/b.rs"]);
    }

    #[test]
    fn test_multi_dependency() {
        let content = r#"# Scroll: Multi Dep

## Task: A

Do A.

## Task: B

Do B.

## Task: C
- depends: A, B

Do C after both A and B.
"#;
        let spec = parse_scroll(content).unwrap();
        assert_eq!(spec.tasks[2].depends_on, vec!["A", "B"]);
    }

    #[test]
    fn test_self_dependency() {
        let content = r#"# Scroll: Self Dep

## Task: A
- depends: A

Do A.
"#;
        let result = parse_scroll(content);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot depend on itself")
        );
    }

    #[test]
    fn test_no_heading() {
        let content = "Just some text without a heading\n";
        assert!(parse_scroll(content).is_err());
    }

    #[test]
    fn test_duplicate_task_names() {
        let content = r#"# Scroll: Dup Names

## Task: Task A

Do A.

## Task: Task A

Do A again.
"#;
        // Duplicate names are allowed by the parser (distinct task IDs assigned later)
        let spec = parse_scroll(content).unwrap();
        assert_eq!(spec.tasks.len(), 2);
    }

    #[test]
    fn test_empty_scroll_name() {
        let content = "# Scroll:\n\n## Task: A\n\nDo A.\n";
        let result = parse_scroll(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn test_task_no_task_text() {
        let content = r#"# Scroll: No Task

## Task: Empty
- files: src/foo.rs
"#;
        let result = parse_scroll(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no task text"));
    }
}
