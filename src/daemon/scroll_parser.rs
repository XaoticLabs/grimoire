//! Markdown spec → ScrollSpec parser.
//!
//! Walks `pulldown-cmark` events rather than line-prefix matching so the
//! grammar can grow without re-implementing markdown semantics (indented
//! list items, fenced code blocks ignored inside prompts, etc.). The
//! on-disk format is unchanged from the legacy parser.

use anyhow::{Result, anyhow};
use pulldown_cmark::{Event, HeadingLevel, Parser, Tag, TagEnd};

/// Parsed spec before insertion into DB.
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
    pub depends_on: Vec<String>,
    /// F5a: when set, the coordinator dispatches this task to the
    /// named peer instead of spawning a local agent. The peer must
    /// have `accept_scroll_dispatch = 1` and lifecycle federation in
    /// the inbound direction back to the coordinator.
    pub peer: Option<String>,
    /// Verification rubric: when set, the worker's completion is
    /// scored by an evaluator agent against this text before the DAG
    /// is allowed to proceed.
    pub verify: Option<String>,
    /// Optional pass threshold (0.0–1.0) for the verification score.
    /// `None` falls back to the keeper's default when `verify` is set.
    pub verify_threshold: Option<f64>,
}

/// Parse a markdown spec file into a [`ScrollSpec`].
///
/// Expected format:
/// ```markdown
/// # Scroll: My Project
/// - workspace: my-workspace
///
/// ## Task: Task Name
/// - files: src/foo.rs, src/bar.rs
/// - depends: Other Task
/// - provider: claude
///
/// The actual task prompt goes here...
/// ```
pub fn parse_scroll(content: &str) -> Result<ScrollSpec> {
    let doc = walk(content)?;

    let name = doc
        .scroll_name
        .ok_or_else(|| anyhow!("Spec must start with a '# Scroll: <name>' heading"))?;
    if name.is_empty() {
        return Err(anyhow!("Scroll name cannot be empty"));
    }

    if doc.tasks.is_empty() {
        return Err(anyhow!(
            "Scroll must contain at least one '## Task:' section"
        ));
    }

    // Reject tasks with no prompt body.
    for task in &doc.tasks {
        if task.prompt.is_empty() {
            return Err(anyhow!(
                "Task '{}' has no task text. Add task description after the metadata lines.",
                task.name
            ));
        }
    }

    // Validate dependency references.
    let task_names: Vec<&str> = doc.tasks.iter().map(|t| t.name.as_str()).collect();
    for task in &doc.tasks {
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
        if task.depends_on.contains(&task.name) {
            return Err(anyhow!("Task '{}' cannot depend on itself", task.name));
        }
    }

    Ok(ScrollSpec {
        name,
        tasks: doc.tasks,
        workspace: doc.workspace,
        workspace_repo: doc.workspace_repo,
        workspace_branch: doc.workspace_branch,
    })
}

/// Working state while consuming markdown events.
#[derive(Default)]
struct Doc {
    scroll_name: Option<String>,
    workspace: Option<String>,
    workspace_repo: Option<String>,
    workspace_branch: Option<String>,
    tasks: Vec<TaskSpec>,
}

#[derive(PartialEq, Eq)]
enum Scope {
    /// Before any `## Task:` heading. List items are workspace directives.
    PreTask,
    /// Inside a `## Task:` section. List items are task metadata until
    /// the first paragraph; then everything is prompt body.
    InTask { metadata_open: bool },
}

fn walk(content: &str) -> Result<Doc> {
    let parser = Parser::new(content);
    let mut doc = Doc::default();
    let mut scope = Scope::PreTask;
    let mut current: Option<TaskSpec> = None;
    let mut prompt_buf = String::new();

    // Heading state.
    let mut h1_buf: Option<String> = None;
    let mut h2_buf: Option<String> = None;

    // List item text accumulator.
    let mut in_list_item = false;
    let mut item_buf = String::new();

    for ev in parser {
        match ev {
            Event::Start(Tag::Heading {
                level: HeadingLevel::H1,
                ..
            }) => {
                h1_buf = Some(String::new());
            }
            Event::End(TagEnd::Heading(HeadingLevel::H1)) => {
                if let Some(raw) = h1_buf.take()
                    && doc.scroll_name.is_none()
                {
                    let cleaned = raw
                        .trim()
                        .trim_start_matches("Scroll:")
                        .trim_start_matches("Scroll: ")
                        .trim();
                    doc.scroll_name = Some(cleaned.to_string());
                }
            }
            Event::Start(Tag::Heading {
                level: HeadingLevel::H2,
                ..
            }) => {
                // Close any in-flight task.
                finalize_task(&mut current, &mut prompt_buf, &mut doc);
                scope = Scope::PreTask; // until we confirm this is a Task: heading
                h2_buf = Some(String::new());
            }
            Event::End(TagEnd::Heading(HeadingLevel::H2)) => {
                if let Some(raw) = h2_buf.take() {
                    let trimmed = raw.trim();
                    if let Some(name) = trimmed
                        .strip_prefix("Task:")
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    {
                        current = Some(TaskSpec {
                            name: name.to_string(),
                            prompt: String::new(),
                            provider: None,
                            model: None,
                            cwd: None,
                            file_patterns: Vec::new(),
                            depends_on: Vec::new(),
                            peer: None,
                            verify: None,
                            verify_threshold: None,
                        });
                        scope = Scope::InTask {
                            metadata_open: true,
                        };
                    }
                }
            }

            Event::Start(Tag::Item) => {
                in_list_item = true;
                item_buf.clear();
            }
            Event::End(TagEnd::Item) => {
                in_list_item = false;
                let line = item_buf.trim().to_string();
                match &mut scope {
                    Scope::PreTask => apply_workspace_directive(&line, &mut doc),
                    Scope::InTask { metadata_open } if *metadata_open => {
                        if let Some(task) = current.as_mut() {
                            apply_task_metadata(&line, task)?;
                        }
                    }
                    Scope::InTask { .. } => {
                        // List items after prompt text begins are part of the prompt.
                        if !prompt_buf.is_empty() {
                            prompt_buf.push('\n');
                        }
                        prompt_buf.push_str("- ");
                        prompt_buf.push_str(&line);
                    }
                }
            }

            Event::Start(Tag::Paragraph) => {
                if let Scope::InTask {
                    ref mut metadata_open,
                } = scope
                {
                    *metadata_open = false;
                    if !prompt_buf.is_empty() {
                        prompt_buf.push_str("\n\n");
                    }
                }
            }

            Event::Text(t) | Event::Code(t) => {
                if let Some(buf) = h1_buf.as_mut() {
                    buf.push_str(&t);
                } else if let Some(buf) = h2_buf.as_mut() {
                    buf.push_str(&t);
                } else if in_list_item {
                    item_buf.push_str(&t);
                } else if matches!(
                    scope,
                    Scope::InTask {
                        metadata_open: false
                    }
                ) {
                    prompt_buf.push_str(&t);
                }
            }
            Event::SoftBreak | Event::HardBreak => {
                if in_list_item {
                    item_buf.push(' ');
                } else if matches!(
                    scope,
                    Scope::InTask {
                        metadata_open: false
                    }
                ) {
                    prompt_buf.push('\n');
                }
            }
            _ => {}
        }
    }

    finalize_task(&mut current, &mut prompt_buf, &mut doc);
    Ok(doc)
}

fn finalize_task(current: &mut Option<TaskSpec>, prompt_buf: &mut String, doc: &mut Doc) {
    if let Some(mut task) = current.take() {
        task.prompt = std::mem::take(prompt_buf).trim().to_string();
        doc.tasks.push(task);
    }
    prompt_buf.clear();
}

fn apply_workspace_directive(line: &str, doc: &mut Doc) {
    if let Some(v) = line.strip_prefix("workspace_repo:") {
        doc.workspace_repo = Some(v.trim().to_string());
    } else if let Some(v) = line.strip_prefix("workspace_branch:") {
        doc.workspace_branch = Some(v.trim().to_string());
    } else if let Some(v) = line.strip_prefix("workspace:") {
        doc.workspace = Some(v.trim().to_string());
    }
}

fn apply_task_metadata(line: &str, task: &mut TaskSpec) -> Result<()> {
    if let Some(v) = line.strip_prefix("files:") {
        task.file_patterns = split_csv(v);
    } else if let Some(v) = line.strip_prefix("depends:") {
        let v = v.trim();
        if !v.is_empty() && v != "(none)" && v != "none" {
            task.depends_on = split_csv(v);
        }
    } else if let Some(v) = line.strip_prefix("provider:") {
        task.provider = Some(v.trim().to_string());
    } else if let Some(v) = line.strip_prefix("model:") {
        task.model = Some(v.trim().to_string());
    } else if let Some(v) = line.strip_prefix("cwd:") {
        task.cwd = Some(v.trim().to_string());
    } else if let Some(v) = line.strip_prefix("peer:") {
        let v = v.trim();
        if !v.is_empty() {
            task.peer = Some(v.to_string());
        }
    } else if let Some(v) = line.strip_prefix("verify_threshold:") {
        let v = v.trim();
        let parsed: f64 = v.parse().map_err(|_| {
            anyhow!(
                "Task '{}': verify_threshold '{}' is not a number between 0.0 and 1.0",
                task.name,
                v
            )
        })?;
        // `contains` is false for NaN, so that path is rejected too.
        if !(0.0..=1.0).contains(&parsed) {
            return Err(anyhow!(
                "Task '{}': verify_threshold {} is out of range (expected 0.0-1.0)",
                task.name,
                parsed
            ));
        }
        task.verify_threshold = Some(parsed);
    } else if let Some(v) = line.strip_prefix("verify:") {
        let v = v.trim();
        if !v.is_empty() {
            task.verify = Some(v.to_string());
        }
    }
    Ok(())
}

fn split_csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_scroll() {
        let content = r"# Scroll: Test Project

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
";

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

    /// F5a: `peer:` directive on a task marks it for cross-peer
    /// dispatch and is preserved on the parsed `TaskSpec`.
    #[test]
    fn peer_directive_attached_to_task() {
        let content = r"# Scroll: Federated

## Task: Remote Build
- peer: build-bot

Run the build on the build-bot peer.
";
        let spec = parse_scroll(content).unwrap();
        assert_eq!(spec.tasks.len(), 1);
        assert_eq!(spec.tasks[0].peer.as_deref(), Some("build-bot"));
    }

    #[test]
    fn test_invalid_dependency() {
        let content = r"# Scroll: Bad Deps

## Task: Task A
- depends: Task C

Do something.
";
        let result = parse_scroll(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("doesn't exist"));
    }

    #[test]
    fn test_empty_scroll() {
        let content = "# Scroll: Empty\n";
        assert!(parse_scroll(content).is_err());
    }

    #[test]
    fn test_all_metadata() {
        let content = r"# Scroll: Full Meta

## Task: Task A
- files: src/a.rs, src/b.rs
- depends: (none)
- provider: aider
- model: gpt-4
- cwd: /home/user

Do task A with all metadata.
";
        let spec = parse_scroll(content).unwrap();
        let task = &spec.tasks[0];
        assert_eq!(task.provider.as_deref(), Some("aider"));
        assert_eq!(task.model.as_deref(), Some("gpt-4"));
        assert_eq!(task.cwd.as_deref(), Some("/home/user"));
        assert_eq!(task.file_patterns, vec!["src/a.rs", "src/b.rs"]);
    }

    #[test]
    fn test_multi_dependency() {
        let content = r"# Scroll: Multi Dep

## Task: A

Do A.

## Task: B

Do B.

## Task: C
- depends: A, B

Do C after both A and B.
";
        let spec = parse_scroll(content).unwrap();
        assert_eq!(spec.tasks[2].depends_on, vec!["A", "B"]);
    }

    #[test]
    fn test_self_dependency() {
        let content = r"# Scroll: Self Dep

## Task: A
- depends: A

Do A.
";
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
        let content = r"# Scroll: Dup Names

## Task: Task A

Do A.

## Task: Task A

Do A again.
";
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
        let content = r"# Scroll: No Task

## Task: Empty
- files: src/foo.rs
";
        let result = parse_scroll(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no task text"));
    }

    #[test]
    fn test_workspace_directives() {
        let content = r"# Scroll: Workspaced
- workspace: my-ws
- workspace_repo: github.com/x/y
- workspace_branch: main

## Task: A

Do A.
";
        let spec = parse_scroll(content).unwrap();
        assert_eq!(spec.workspace.as_deref(), Some("my-ws"));
        assert_eq!(spec.workspace_repo.as_deref(), Some("github.com/x/y"));
        assert_eq!(spec.workspace_branch.as_deref(), Some("main"));
    }
}
