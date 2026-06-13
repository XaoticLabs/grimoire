//! `grim mcp` — an MCP server over stdio that proxies to the daemon's UDS RPC,
//! so any MCP client can drive agents without speaking grimoire's protocol.
//!
//! Speaks spec revision 2025-11-25 including the experimental **Tasks** utility:
//! a task-augmented `summon_agent` returns a task whose `taskId` *is* the agent
//! id; `tasks/get` maps agent states onto task statuses, `tasks/result` blocks
//! until terminal. The daemon's queue was already shaped this way.
//!
//! Transport is stdio (newline-delimited JSON-RPC 2.0): the host spawns
//! `grim mcp` per session, so the requestor is implicit and task access is
//! session-bounded. Diagnostics go to stderr; stdout carries frames only.

use std::collections::HashMap;
use std::io::Write;

use anyhow::Result;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{AgentResultResponse, CircleResult, SummonResult};

/// Protocol revisions we can speak. Tasks are only advertised on 2025-11-25.
const SUPPORTED_VERSIONS: [&str; 3] = ["2025-11-25", "2025-06-18", "2025-03-26"];
const LATEST_VERSION: &str = "2025-11-25";

/// Suggested client polling cadence for task status (ms).
const POLL_INTERVAL_MS: u64 = 2000;
/// Hard ceiling on how long `tasks/result` will block on a running agent.
const RESULT_BLOCK_MAX: std::time::Duration = std::time::Duration::from_hours(1);

struct Session {
    client: DaemonClient,
    /// taskId (agent id) → ISO-8601 createdAt. Session-scoped: stdio = one requestor.
    tasks: HashMap<String, String>,
    /// Negotiated protocol revision (set by `initialize`).
    version: String,
}

pub async fn run() -> Result<()> {
    let client = DaemonClient::connect().await?;
    let mut session = Session {
        client,
        tasks: HashMap::new(),
        version: LATEST_VERSION.to_string(),
    };

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        if let Some(response) = session.handle_line(&line).await {
            println!("{response}");
            std::io::stdout().flush().ok();
        }
    }
    Ok(())
}

fn rpc_result(id: &Value, result: Value) -> String {
    json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string()
}

fn rpc_error(id: &Value, code: i64, message: &str) -> String {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}}).to_string()
}

/// Map an agent state onto an MCP task status.
fn task_status(agent_state: &str) -> &'static str {
    match agent_state {
        "Complete" | "Dormant" => "completed",
        "Failed" => "failed",
        "Banished" => "cancelled",
        _ => "working",
    }
}

const fn status_is_terminal(status: &str) -> bool {
    matches!(status.as_bytes(), b"completed" | b"failed" | b"cancelled")
}

impl Session {
    async fn handle_line(&mut self, line: &str) -> Option<String> {
        let msg: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => return Some(rpc_error(&Value::Null, -32700, "parse error")),
        };
        let method = msg.get("method").and_then(Value::as_str)?.to_string();
        let id = msg.get("id").cloned();
        let params = msg.get("params").cloned().unwrap_or_else(|| json!({}));

        // Notifications (no id) get no response.
        let id = id?;

        let out = match method.as_str() {
            "initialize" => self.initialize(&id, &params),
            "ping" => rpc_result(&id, json!({})),
            "tools/list" => rpc_result(&id, self.tools_list()),
            "tools/call" => self.tools_call(&id, &params).await,
            "tasks/get" => self.tasks_get(&id, &params).await,
            "tasks/result" => self.tasks_result(&id, &params).await,
            "tasks/cancel" => self.tasks_cancel(&id, &params).await,
            "tasks/list" => self.tasks_list(&id).await,
            _ => rpc_error(&id, -32601, "method not found"),
        };
        Some(out)
    }

    fn initialize(&mut self, id: &Value, params: &Value) -> String {
        let requested = params
            .get("protocolVersion")
            .and_then(Value::as_str)
            .unwrap_or(LATEST_VERSION);
        self.version = if SUPPORTED_VERSIONS.contains(&requested) {
            requested.to_string()
        } else {
            LATEST_VERSION.to_string()
        };
        let mut capabilities = json!({"tools": {}});
        if self.version == "2025-11-25" {
            capabilities["tasks"] = json!({
                "list": {},
                "cancel": {},
                "requests": {"tools": {"call": {}}}
            });
        }
        rpc_result(
            id,
            json!({
                "protocolVersion": self.version,
                "capabilities": capabilities,
                "serverInfo": {
                    "name": "grimoire",
                    "title": "Grimoire agent daemon",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "instructions": "Supervise long-lived coding agents. summon_agent starts one \
                    (task-augment the call to get a pollable task whose result is the agent's \
                    final output); agent_status / agent_result / agent_chronicle inspect it; \
                    send_mail reaches a standing agent; banish_agent stops one.",
            }),
        )
    }

    fn tools_list(&self) -> Value {
        let mut summon = json!({
            "name": "summon_agent",
            "description": "Summon a supervised coding agent under the grimoire daemon. \
                Returns immediately with the agent id; the agent runs in the background. \
                Task-augment this call (MCP Tasks) to poll it and fetch its final output.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "task": {"type": "string", "description": "The prompt / task for the agent"},
                    "provider": {"type": "string", "description": "CLI provider (e.g. claude, pi, codex)"},
                    "model": {"type": "string"},
                    "cwd": {"type": "string", "description": "Working directory for the agent"},
                    "name": {"type": "string"},
                    "keep_alive": {"type": "boolean", "description": "Park in Dormant after the first run so wake sources can resume it"}
                },
                "required": ["task"]
            }
        });
        if self.version == "2025-11-25" {
            summon["execution"] = json!({"taskSupport": "optional"});
        }
        json!({"tools": [
            summon,
            {
                "name": "agent_status",
                "description": "Current state of one agent (or all agents when id is omitted).",
                "inputSchema": {"type": "object", "properties": {"id": {"type": "string"}}}
            },
            {
                "name": "agent_result",
                "description": "Final output text of an agent, if it has finished.",
                "inputSchema": {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}
            },
            {
                "name": "agent_chronicle",
                "description": "Replay an agent's durable event log (lifecycle + output).",
                "inputSchema": {"type": "object", "properties": {
                    "id": {"type": "string"},
                    "last": {"type": "integer", "description": "Only the last N entries"}
                }, "required": ["id"]}
            },
            {
                "name": "send_mail",
                "description": "Send mail to agent://<id> or topic://<name>. Dormant agents wake on mail.",
                "inputSchema": {"type": "object", "properties": {
                    "to": {"type": "string"}, "body": {"type": "string"}
                }, "required": ["to", "body"]}
            },
            {
                "name": "banish_agent",
                "description": "Stop and retire an agent (cascades its wake sources and children).",
                "inputSchema": {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}
            }
        ]})
    }

    async fn tools_call(&mut self, id: &Value, params: &Value) -> String {
        let name = params.get("name").and_then(Value::as_str).unwrap_or("");
        let args = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let as_task = params.get("task").is_some();

        if as_task && name != "summon_agent" {
            return rpc_error(id, -32601, "tool does not support task augmentation");
        }
        if as_task && self.version != "2025-11-25" {
            return rpc_error(id, -32601, "tasks require protocol 2025-11-25");
        }

        match name {
            "summon_agent" => self.call_summon(id, &args, as_task).await,
            "agent_status" => self.call_status(id, &args).await,
            "agent_result" => self.call_result(id, &args).await,
            "agent_chronicle" => self.call_chronicle(id, &args).await,
            "send_mail" => self.call_send_mail(id, &args).await,
            "banish_agent" => self.call_banish(id, &args).await,
            _ => rpc_error(id, -32602, "unknown tool"),
        }
    }

    async fn call_summon(&mut self, id: &Value, args: &Value, as_task: bool) -> String {
        let summon_params = json!({
            "task": args.get("task").and_then(Value::as_str).unwrap_or(""),
            "name": args.get("name"),
            "model": args.get("model"),
            "provider": args.get("provider"),
            "cwd": args.get("cwd"),
            "keep_alive": args.get("keep_alive").and_then(Value::as_bool),
        });
        let result: Result<SummonResult> =
            self.client.call_typed("agent.summon", summon_params).await;
        match result {
            Ok(r) if as_task => {
                let now = chrono::Utc::now().to_rfc3339();
                self.tasks.insert(r.id.clone(), now.clone());
                rpc_result(
                    id,
                    json!({"task": {
                        "taskId": r.id,
                        "status": "working",
                        "statusMessage": format!("agent summoned (state: {})", r.state),
                        "createdAt": now,
                        "lastUpdatedAt": now,
                        "ttl": null,
                        "pollInterval": POLL_INTERVAL_MS,
                    }}),
                )
            }
            Ok(r) => tool_text(
                id,
                false,
                &format!(
                    "Summoned agent {} (state: {}). Poll with agent_status, fetch output \
                     with agent_result, or task-augment summon_agent next time.",
                    r.id, r.state
                ),
            ),
            Err(e) => tool_text(id, true, &format!("summon failed: {e}")),
        }
    }

    async fn call_status(&mut self, id: &Value, args: &Value) -> String {
        let circle: Result<CircleResult> = self
            .client
            .call_typed("agent.circle", json!({"state": null}))
            .await;
        match circle {
            Ok(c) => {
                let filter = args.get("id").and_then(Value::as_str);
                let lines: Vec<String> = c
                    .agents
                    .iter()
                    .filter(|a| filter.is_none_or(|f| a.id.starts_with(f)))
                    .map(|a| {
                        format!(
                            "{}  {}  {}  {}",
                            a.id,
                            a.name.as_deref().unwrap_or("-"),
                            a.state,
                            a.task.as_deref().unwrap_or("-")
                        )
                    })
                    .collect();
                if lines.is_empty() {
                    tool_text(id, false, "no matching agents")
                } else {
                    tool_text(id, false, &lines.join("\n"))
                }
            }
            Err(e) => tool_text(id, true, &format!("status failed: {e}")),
        }
    }

    async fn call_result(&mut self, id: &Value, args: &Value) -> String {
        let agent_id = args.get("id").and_then(Value::as_str).unwrap_or("");
        let resp: Result<AgentResultResponse> = self
            .client
            .call_typed("agent.result", json!({"id": agent_id}))
            .await;
        match resp {
            Ok(r) => match r.result {
                Some(text) => tool_text(id, false, &text),
                None => tool_text(
                    id,
                    false,
                    &format!("agent is {} with no result text yet", r.state),
                ),
            },
            Err(e) => tool_text(id, true, &format!("result failed: {e}")),
        }
    }

    async fn call_chronicle(&mut self, id: &Value, args: &Value) -> String {
        let agent_id = args.get("id").and_then(Value::as_str).unwrap_or("");
        let resp = self
            .client
            .call("agent.replay", json!({"id": agent_id}))
            .await;
        match resp {
            Ok(r) if r.error.is_none() => {
                let entries = r
                    .result
                    .as_ref()
                    .and_then(|v| v.get("entries"))
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let last = args
                    .get("last")
                    .and_then(Value::as_u64)
                    .map_or(entries.len(), |n| n as usize);
                let skip = entries.len().saturating_sub(last);
                let text = entries
                    .iter()
                    .skip(skip)
                    .map(|e| {
                        format!(
                            "[{}] {} {}",
                            e.get("seq").and_then(Value::as_i64).unwrap_or(0),
                            e.get("kind").and_then(Value::as_str).unwrap_or("?"),
                            e.get("payload")
                                .map(ToString::to_string)
                                .unwrap_or_default()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                tool_text(id, false, &text)
            }
            Ok(r) => tool_text(
                id,
                true,
                &r.error
                    .map_or_else(|| "replay failed".into(), |e| e.message),
            ),
            Err(e) => tool_text(id, true, &format!("replay failed: {e}")),
        }
    }

    async fn call_send_mail(&mut self, id: &Value, args: &Value) -> String {
        let resp = self
            .client
            .call(
                "mail.send",
                json!({
                    "to": args.get("to").and_then(Value::as_str).unwrap_or(""),
                    "body": args.get("body").and_then(Value::as_str).unwrap_or(""),
                }),
            )
            .await;
        match resp {
            Ok(r) if r.error.is_none() => tool_text(id, false, "mail sent"),
            Ok(r) => tool_text(
                id,
                true,
                &r.error.map_or_else(|| "send failed".into(), |e| e.message),
            ),
            Err(e) => tool_text(id, true, &format!("send failed: {e}")),
        }
    }

    async fn call_banish(&mut self, id: &Value, args: &Value) -> String {
        let agent_id = args.get("id").and_then(Value::as_str).unwrap_or("");
        let resp = self
            .client
            .call("agent.banish", json!({"id": agent_id}))
            .await;
        match resp {
            Ok(r) if r.error.is_none() => tool_text(id, false, &format!("banished {agent_id}")),
            Ok(r) => tool_text(
                id,
                true,
                &r.error
                    .map_or_else(|| "banish failed".into(), |e| e.message),
            ),
            Err(e) => tool_text(id, true, &format!("banish failed: {e}")),
        }
    }

    // --- Tasks utility (2025-11-25, experimental) --------------------------

    /// Build the task object for a session task, or an error string.
    async fn task_object(&mut self, task_id: &str) -> Result<Value, String> {
        let created_at = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| "Failed to retrieve task: Task not found".to_string())?;
        let resp: Result<AgentResultResponse> = self
            .client
            .call_typed("agent.result", json!({"id": task_id}))
            .await;
        let state = match resp {
            Ok(r) => r.state,
            Err(e) => return Err(format!("Failed to retrieve task: {e}")),
        };
        let status = task_status(&state);
        Ok(json!({
            "taskId": task_id,
            "status": status,
            "statusMessage": format!("agent state: {state}"),
            "createdAt": created_at,
            "lastUpdatedAt": chrono::Utc::now().to_rfc3339(),
            "ttl": null,
            "pollInterval": POLL_INTERVAL_MS,
        }))
    }

    async fn tasks_get(&mut self, id: &Value, params: &Value) -> String {
        let task_id = params.get("taskId").and_then(Value::as_str).unwrap_or("");
        match self.task_object(task_id).await {
            Ok(task) => rpc_result(id, task),
            Err(msg) => rpc_error(id, -32602, &msg),
        }
    }

    async fn tasks_result(&mut self, id: &Value, params: &Value) -> String {
        let task_id = params
            .get("taskId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if !self.tasks.contains_key(&task_id) {
            return rpc_error(id, -32602, "Failed to retrieve task: Task not found");
        }
        // Block until terminal, per spec, with a hard ceiling.
        let deadline = std::time::Instant::now() + RESULT_BLOCK_MAX;
        loop {
            let resp: Result<AgentResultResponse> = self
                .client
                .call_typed("agent.result", json!({"id": task_id}))
                .await;
            let r = match resp {
                Ok(r) => r,
                Err(e) => return rpc_error(id, -32603, &format!("internal error: {e}")),
            };
            let status = task_status(&r.state);
            if status_is_terminal(status) {
                let text = r
                    .result
                    .unwrap_or_else(|| format!("(agent finished as {} with no output)", r.state));
                return rpc_result(
                    id,
                    json!({
                        "content": [{"type": "text", "text": text}],
                        "isError": status == "failed",
                        "_meta": {"io.modelcontextprotocol/related-task": {"taskId": task_id}},
                    }),
                );
            }
            if std::time::Instant::now() >= deadline {
                return rpc_error(id, -32603, "internal error: result wait timed out");
            }
            tokio::time::sleep(std::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    }

    async fn tasks_cancel(&mut self, id: &Value, params: &Value) -> String {
        let task_id = params.get("taskId").and_then(Value::as_str).unwrap_or("");
        let task = match self.task_object(task_id).await {
            Ok(t) => t,
            Err(msg) => return rpc_error(id, -32602, &msg),
        };
        let status = task["status"].as_str().unwrap_or("working");
        if status_is_terminal(status) {
            return rpc_error(
                id,
                -32602,
                &format!("Cannot cancel task: already in terminal status '{status}'"),
            );
        }
        if let Err(e) = self
            .client
            .call("agent.banish", json!({"id": task_id}))
            .await
        {
            return rpc_error(id, -32603, &format!("internal error: {e}"));
        }
        let mut task = task;
        task["status"] = json!("cancelled");
        task["statusMessage"] = json!("The task was cancelled by request.");
        task["lastUpdatedAt"] = json!(chrono::Utc::now().to_rfc3339());
        rpc_result(id, task)
    }

    async fn tasks_list(&mut self, id: &Value) -> String {
        let ids: Vec<String> = self.tasks.keys().cloned().collect();
        let mut tasks = Vec::new();
        for task_id in ids {
            if let Ok(t) = self.task_object(&task_id).await {
                tasks.push(t);
            }
        }
        rpc_result(id, json!({"tasks": tasks}))
    }
}

/// A `CallToolResult` with a single text content item.
fn tool_text(id: &Value, is_error: bool, text: &str) -> String {
    rpc_result(
        id,
        json!({"content": [{"type": "text", "text": text}], "isError": is_error}),
    )
}
