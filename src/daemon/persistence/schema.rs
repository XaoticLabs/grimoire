//! Schema bootstrap and forward-only migrations for the daemon database.

use anyhow::Result;
use rusqlite::Connection;

use super::Database;

/// Idempotent `ALTER TABLE ADD COLUMN` for forward-only migrations.
/// `column_ddl` is the full DDL fragment; all identifiers are crate literals
/// (no injection surface).
pub(super) fn add_column_if_missing(
    conn: &Connection,
    table: &str,
    column: &str,
    column_ddl: &str,
) -> Result<()> {
    let probe = format!("SELECT {column} FROM {table} LIMIT 0");
    if conn.prepare(&probe).is_err() {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column_ddl};"))?;
    }
    Ok(())
}

impl Database {
    pub(super) fn migrate(&self) -> Result<()> {
        let conn = self.conn_lock();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS agents (
                id          TEXT PRIMARY KEY,
                name        TEXT,
                state       TEXT NOT NULL,
                task        TEXT,
                model       TEXT,
                provider    TEXT,
                cwd         TEXT NOT NULL,
                pid         INTEGER,
                session_id  TEXT,
                exit_code   INTEGER,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL,
                worker_id   TEXT
            );

            CREATE TABLE IF NOT EXISTS agent_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id    TEXT NOT NULL REFERENCES agents(id),
                event_type  TEXT NOT NULL,
                payload     TEXT NOT NULL,
                created_at  TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_agent_events_agent_id
                ON agent_events(agent_id);

            CREATE TABLE IF NOT EXISTS pacts (
                id          TEXT PRIMARY KEY,
                source_id   TEXT NOT NULL REFERENCES agents(id),
                task_tpl    TEXT NOT NULL,
                name        TEXT,
                state       TEXT NOT NULL DEFAULT 'pending',
                target_id   TEXT,
                created_at  TEXT NOT NULL,
                fired_at    TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_pacts_source_id
                ON pacts(source_id);
            ",
        )?;

        add_column_if_missing(&conn, "agents", "provider", "provider TEXT")?;
        add_column_if_missing(&conn, "agents", "worker_id", "worker_id TEXT")?;

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS scrolls (
                id              TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                state           TEXT NOT NULL DEFAULT 'inscribed',
                source_path     TEXT,
                max_concurrency INTEGER NOT NULL DEFAULT 4,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id              TEXT PRIMARY KEY,
                scroll_id       TEXT NOT NULL REFERENCES scrolls(id),
                name            TEXT NOT NULL,
                prompt          TEXT NOT NULL,
                state           TEXT NOT NULL DEFAULT 'blocked',
                agent_id        TEXT,
                provider        TEXT,
                model           TEXT,
                cwd             TEXT,
                file_patterns   TEXT NOT NULL DEFAULT '[]',
                order_index     INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_scroll_id ON tasks(scroll_id);
            CREATE INDEX IF NOT EXISTS idx_tasks_agent_id ON tasks(agent_id);

            CREATE TABLE IF NOT EXISTS task_dependencies (
                task_id         TEXT NOT NULL REFERENCES tasks(id),
                depends_on_id   TEXT NOT NULL REFERENCES tasks(id),
                PRIMARY KEY (task_id, depends_on_id)
            );

            CREATE TABLE IF NOT EXISTS events (
                id          INTEGER PRIMARY KEY,
                agent_id    TEXT,
                scroll_id   TEXT,
                seq         INTEGER NOT NULL,
                kind        TEXT NOT NULL,
                payload     TEXT NOT NULL,
                ts          TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_agent_seq  ON events(agent_id, seq);
            CREATE INDEX IF NOT EXISTS idx_events_scroll_seq ON events(scroll_id, seq);

            CREATE TABLE IF NOT EXISTS task_queue (
                id              TEXT PRIMARY KEY,
                lane            TEXT NOT NULL,
                priority        INTEGER NOT NULL DEFAULT 0,
                enqueued_at     TEXT NOT NULL,
                provider_name   TEXT,
                cwd             TEXT NOT NULL,
                model           TEXT,
                task_text       TEXT NOT NULL,
                block_reason    TEXT,
                FOREIGN KEY (id) REFERENCES agents(id)
            );
            CREATE INDEX IF NOT EXISTS idx_task_queue_dispatch
                ON task_queue (lane, priority DESC, enqueued_at, id);

            CREATE TABLE IF NOT EXISTS mail (
                id              TEXT PRIMARY KEY,
                recipient_id    TEXT NOT NULL,
                sender_id       TEXT,
                topic           TEXT,
                body            TEXT NOT NULL,
                in_reply_to     TEXT,
                state           TEXT NOT NULL,
                fail_reason     TEXT,
                created_at      INTEGER NOT NULL,
                delivered_at    INTEGER,
                seq             INTEGER NOT NULL,
                wake_eligible   INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS mail_by_recipient ON mail (recipient_id, seq);
            CREATE INDEX IF NOT EXISTS mail_pending_wake ON mail (recipient_id, state) WHERE state = 'Pending' AND wake_eligible = 1;

            CREATE TABLE IF NOT EXISTS subscriptions (
                id              TEXT PRIMARY KEY,
                subscriber_id   TEXT NOT NULL,
                topic           TEXT NOT NULL,
                created_at      INTEGER NOT NULL,
                UNIQUE (subscriber_id, topic)
            );
            CREATE INDEX IF NOT EXISTS subs_by_topic ON subscriptions (topic);

            CREATE TABLE IF NOT EXISTS wake_sources (
                id              TEXT PRIMARY KEY,
                agent_id        TEXT NOT NULL,
                kind            TEXT NOT NULL,
                config_json     TEXT NOT NULL,
                state           TEXT NOT NULL,
                fail_reason     TEXT,
                last_fired_at   INTEGER,
                fire_count      INTEGER NOT NULL DEFAULT 0,
                created_at      INTEGER NOT NULL,
                FOREIGN KEY(agent_id) REFERENCES agents(id)
            );
            CREATE INDEX IF NOT EXISTS wake_sources_by_agent ON wake_sources(agent_id);
            CREATE INDEX IF NOT EXISTS wake_sources_armed
                ON wake_sources(state) WHERE state = 'armed';

            CREATE TABLE IF NOT EXISTS wake_rate_limits (
                agent_id        TEXT PRIMARY KEY,
                tokens          REAL NOT NULL,
                last_refill_at  INTEGER NOT NULL,
                capacity        INTEGER NOT NULL DEFAULT 60,
                refill_per_sec  REAL NOT NULL DEFAULT 0.01666666,
                FOREIGN KEY(agent_id) REFERENCES agents(id)
            );

            -- One USD ceiling per supervision-tree root; spend is summed over
            -- the subtree at enforcement. `exhausted_at` is set once (fire-once).
            CREATE TABLE IF NOT EXISTS tree_budgets (
                root_agent_id   TEXT PRIMARY KEY REFERENCES agents(id),
                cap_usd         REAL NOT NULL,
                exhausted_at    TEXT
            );

            CREATE TABLE IF NOT EXISTS restart_history (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id        TEXT NOT NULL REFERENCES agents(id),
                attempted_at    INTEGER NOT NULL,
                outcome         TEXT NOT NULL,
                error_summary   TEXT
            );
            CREATE INDEX IF NOT EXISTS restart_history_by_agent_window
                ON restart_history(agent_id, attempted_at);
            CREATE INDEX IF NOT EXISTS restart_history_by_time
                ON restart_history(attempted_at);
            ",
        )?;

        add_column_if_missing(
            &conn,
            "agents",
            "keep_alive",
            "keep_alive INTEGER NOT NULL DEFAULT 0",
        )?;
        add_column_if_missing(
            &conn,
            "agents",
            "restart_policy",
            "restart_policy TEXT NOT NULL DEFAULT 'never'",
        )?;
        add_column_if_missing(&conn, "agents", "max_restarts", "max_restarts INTEGER")?;
        add_column_if_missing(
            &conn,
            "agents",
            "restart_window_secs",
            "restart_window_secs INTEGER",
        )?;
        add_column_if_missing(&conn, "agents", "escalate_to", "escalate_to TEXT")?;
        add_column_if_missing(
            &conn,
            "agents",
            "restart_count",
            "restart_count INTEGER NOT NULL DEFAULT 0",
        )?;
        add_column_if_missing(
            &conn,
            "agents",
            "escalation_depth",
            "escalation_depth INTEGER NOT NULL DEFAULT 0",
        )?;
        // Cumulative input+output tokens across all turns; `SandboxConfig
        // .token_budget` gates the next dispatch against this.
        add_column_if_missing(
            &conn,
            "agents",
            "tokens_used",
            "tokens_used INTEGER NOT NULL DEFAULT 0",
        )?;
        // Supervision-tree link: when set, this agent dies with its parent.
        // DB-only (out of `Agent`); read only on a parent's banish.
        add_column_if_missing(&conn, "agents", "parent_agent_id", "parent_agent_id TEXT")?;
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_agents_parent ON agents(parent_agent_id);",
        )?;
        // Lifetime USD spend, charged at each run-completion from
        // `tokens_used` × `[providers.<name>.pricing]`.
        add_column_if_missing(
            &conn,
            "agents",
            "usd_spent",
            "usd_spent REAL NOT NULL DEFAULT 0",
        )?;
        // Per-budget, per-UTC-day spend ledger; composite PK so `today` is a
        // single indexed row read.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS budget_spend (
                budget_name TEXT NOT NULL,
                day         TEXT NOT NULL,
                usd         REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (budget_name, day)
            );",
        )?;
        // Rubric-scored evaluations, many-per-target, keyed by synthetic id.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS eval_results (
                id           TEXT PRIMARY KEY,
                target_id    TEXT NOT NULL,
                evaluator_id TEXT NOT NULL,
                score        REAL NOT NULL,
                verdict      TEXT,
                rationale    TEXT,
                created_at   INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_eval_results_target
                ON eval_results(target_id, created_at);",
        )?;

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS workspaces (
                id          TEXT PRIMARY KEY,
                path        TEXT NOT NULL UNIQUE,
                repo_path   TEXT NOT NULL,
                branch      TEXT NOT NULL,
                state       TEXT NOT NULL,
                created_at  INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS workspace_memory (
                workspace_id TEXT NOT NULL,
                key          TEXT NOT NULL,
                value        BLOB NOT NULL,
                version      INTEGER NOT NULL,
                updated_at   INTEGER NOT NULL,
                updated_by   TEXT NOT NULL,
                PRIMARY KEY (workspace_id, key),
                FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS workspace_memory_by_prefix
                ON workspace_memory (workspace_id, key);

            CREATE TABLE IF NOT EXISTS workspace_assignments (
                workspace_id TEXT NOT NULL,
                agent_id     TEXT NOT NULL,
                assigned_at  INTEGER NOT NULL,
                PRIMARY KEY (workspace_id, agent_id),
                FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
                FOREIGN KEY (agent_id)     REFERENCES agents(id)
            );
            CREATE INDEX IF NOT EXISTS workspace_assignments_by_agent
                ON workspace_assignments (agent_id);

            CREATE TABLE IF NOT EXISTS peers (
                id                  TEXT PRIMARY KEY,
                daemon_id           TEXT NOT NULL,
                name                TEXT NOT NULL UNIQUE,
                url                 TEXT NOT NULL,
                bearer_token_hash   BLOB NOT NULL UNIQUE,
                bearer_token        TEXT NOT NULL,
                public_key          BLOB,
                state               TEXT NOT NULL,
                last_seen           INTEGER,
                registered_at       INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS peers_by_daemon_id ON peers(daemon_id);

            CREATE TABLE IF NOT EXISTS peer_outbox (
                id              TEXT PRIMARY KEY,
                peer_id         TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                mail_id         TEXT NOT NULL,
                sender_seq      INTEGER NOT NULL,
                recipient       TEXT NOT NULL,
                sender          TEXT,
                topic           TEXT,
                body            TEXT NOT NULL,
                created_at      INTEGER NOT NULL,
                attempts        INTEGER NOT NULL DEFAULT 0,
                next_attempt_at INTEGER NOT NULL,
                state           TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS peer_outbox_drain
                ON peer_outbox(peer_id, state, next_attempt_at);
            CREATE UNIQUE INDEX IF NOT EXISTS peer_outbox_seq
                ON peer_outbox(peer_id, sender_seq);

            CREATE TABLE IF NOT EXISTS peer_inbox (
                sender_daemon_id TEXT NOT NULL,
                sender_seq       INTEGER NOT NULL,
                mail_id          TEXT NOT NULL,
                received_at      INTEGER NOT NULL,
                PRIMARY KEY (sender_daemon_id, sender_seq)
            );

            CREATE TABLE IF NOT EXISTS topic_federations (
                id          TEXT PRIMARY KEY,
                peer_id     TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                topic       TEXT NOT NULL,
                direction   TEXT NOT NULL,
                created_at  INTEGER NOT NULL,
                UNIQUE (peer_id, topic)
            );
            CREATE INDEX IF NOT EXISTS topic_federations_by_topic
                ON topic_federations(topic);

            -- Federated namespace KV store. Conflict resolution is LWW on
            -- (lamport, origin_daemon_id); deletes are tombstones (deleted=1)
            -- so they propagate by the same rule.
            CREATE TABLE IF NOT EXISTS namespace_memory (
                namespace        TEXT NOT NULL,
                key              TEXT NOT NULL,
                value            BLOB NOT NULL,
                lamport          INTEGER NOT NULL,
                origin_daemon_id TEXT NOT NULL,
                deleted          INTEGER NOT NULL DEFAULT 0,
                updated_at       INTEGER NOT NULL,
                updated_by       TEXT NOT NULL,
                PRIMARY KEY (namespace, key)
            );
            CREATE INDEX IF NOT EXISTS namespace_memory_by_key
                ON namespace_memory(namespace, key);

            -- Per-daemon Lamport clock (single row); advances on local writes
            -- and on observing a remote timestamp.
            CREATE TABLE IF NOT EXISTS namespace_lamport (
                node    INTEGER PRIMARY KEY CHECK (node = 0),
                counter INTEGER NOT NULL
            );
            INSERT OR IGNORE INTO namespace_lamport (node, counter) VALUES (0, 0);

            -- Which peers a namespace replicates to, and the direction.
            CREATE TABLE IF NOT EXISTS namespace_federations (
                id          TEXT PRIMARY KEY,
                peer_id     TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                namespace   TEXT NOT NULL,
                direction   TEXT NOT NULL,
                created_at  INTEGER NOT NULL,
                UNIQUE (peer_id, namespace)
            );
            CREATE INDEX IF NOT EXISTS namespace_federations_by_ns
                ON namespace_federations(namespace);

            -- Per-peer queue of namespace writes awaiting replication;
            -- redelivery is safe because LWW apply is idempotent.
            CREATE TABLE IF NOT EXISTS namespace_outbox (
                id               TEXT PRIMARY KEY,
                peer_id          TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                op_id            TEXT NOT NULL,
                namespace        TEXT NOT NULL,
                key              TEXT NOT NULL,
                value            BLOB NOT NULL,
                lamport          INTEGER NOT NULL,
                origin_daemon_id TEXT NOT NULL,
                deleted          INTEGER NOT NULL,
                updated_by       TEXT NOT NULL,
                created_at       INTEGER NOT NULL,
                attempts         INTEGER NOT NULL DEFAULT 0,
                next_attempt_at  INTEGER NOT NULL,
                state            TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS namespace_outbox_drain
                ON namespace_outbox(peer_id, state, next_attempt_at);
            ",
        )?;

        add_column_if_missing(&conn, "agents", "workspace_id", "workspace_id TEXT")?;

        // Shadow workspaces have no worktree; `path` holds a sentinel
        // `shadow://<home-id>/<ws-id>` to satisfy the UNIQUE constraint.
        add_column_if_missing(
            &conn,
            "workspaces",
            "kind",
            "kind TEXT NOT NULL DEFAULT 'Local'",
        )?;
        add_column_if_missing(&conn, "workspaces", "home_daemon_id", "home_daemon_id TEXT")?;
        add_column_if_missing(
            &conn,
            "workspaces",
            "home_workspace_id",
            "home_workspace_id TEXT",
        )?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS workspace_federations (
                id           TEXT PRIMARY KEY,
                peer_id      TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                workspace_id TEXT NOT NULL,
                direction    TEXT NOT NULL,
                created_at   INTEGER NOT NULL,
                UNIQUE (peer_id, workspace_id)
             );
             CREATE INDEX IF NOT EXISTS workspace_federations_by_ws
                ON workspace_federations(workspace_id);",
        )?;

        // Per-peer outbox for workspace file events. `sender_seq` is the
        // monotonic correlation key acked back; `payload` is the
        // JSON-serialized WorkspaceFileChanged batch.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS workspace_event_outbox (
                id              TEXT PRIMARY KEY,
                peer_id         TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                workspace_id    TEXT NOT NULL,
                sender_seq      INTEGER NOT NULL,
                payload         BLOB NOT NULL,
                created_at      INTEGER NOT NULL,
                attempts        INTEGER NOT NULL DEFAULT 0,
                next_attempt_at INTEGER NOT NULL,
                state           TEXT NOT NULL
             );
             CREATE UNIQUE INDEX IF NOT EXISTS workspace_event_outbox_seq
                ON workspace_event_outbox(peer_id, sender_seq);
             CREATE INDEX IF NOT EXISTS workspace_event_outbox_due
                ON workspace_event_outbox(peer_id, state, next_attempt_at);",
        )?;

        // Receiver-side dedupe keyed on `(sender_daemon_id, sender_seq)`: the
        // sender retries until acked, so replays must be ignored.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS workspace_event_inbox (
                sender_daemon_id TEXT NOT NULL,
                sender_seq       INTEGER NOT NULL,
                workspace_id     TEXT NOT NULL,
                received_at      INTEGER NOT NULL,
                PRIMARY KEY (sender_daemon_id, sender_seq)
             );
             CREATE INDEX IF NOT EXISTS workspace_event_inbox_by_ws
                ON workspace_event_inbox(workspace_id);",
        )?;

        // Peer the task is dispatched to; NULL ⇒ local task.
        add_column_if_missing(&conn, "tasks", "peer_name", "peer_name TEXT")?;

        // Verification-gating columns; NULL `verify_rubric` ⇒ ordinary task
        // that completes on worker completion.
        add_column_if_missing(&conn, "tasks", "verify_rubric", "verify_rubric TEXT")?;
        add_column_if_missing(&conn, "tasks", "verify_threshold", "verify_threshold REAL")?;
        add_column_if_missing(
            &conn,
            "tasks",
            "verifier_agent_id",
            "verifier_agent_id TEXT",
        )?;

        // Opt-in flag: the dispatch handler refuses inbound `ScrollTaskDispatch`
        // from peers not enrolled here.
        add_column_if_missing(
            &conn,
            "peers",
            "accept_scroll_dispatch",
            "accept_scroll_dispatch INTEGER NOT NULL DEFAULT 0",
        )?;

        // Durable record of (scroll, task, peer) dispatches, separate from the
        // at-least-once wire layer (scroll_dispatch_outbox/inbox).
        // `remote_agent_id` filled on ack; `state` is
        // `pending` → `dispatched` → `complete`/`failed`/`cancelled`.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS scroll_task_dispatches (
                id              TEXT PRIMARY KEY,
                scroll_id       TEXT NOT NULL,
                task_id         TEXT NOT NULL,
                peer_id         TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                remote_agent_id TEXT,
                state           TEXT NOT NULL,
                created_at      INTEGER NOT NULL,
                updated_at      INTEGER NOT NULL,
                UNIQUE (scroll_id, task_id, peer_id)
             );
             CREATE INDEX IF NOT EXISTS scroll_task_dispatches_by_task
                ON scroll_task_dispatches(task_id);
             CREATE INDEX IF NOT EXISTS scroll_task_dispatches_by_remote
                ON scroll_task_dispatches(peer_id, remote_agent_id);

             CREATE TABLE IF NOT EXISTS scroll_dispatch_outbox (
                id              TEXT PRIMARY KEY,
                peer_id         TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                sender_seq      INTEGER NOT NULL,
                payload         BLOB NOT NULL,
                created_at      INTEGER NOT NULL,
                attempts        INTEGER NOT NULL DEFAULT 0,
                next_attempt_at INTEGER NOT NULL,
                state           TEXT NOT NULL
             );
             CREATE UNIQUE INDEX IF NOT EXISTS scroll_dispatch_outbox_seq
                ON scroll_dispatch_outbox(peer_id, sender_seq);
             CREATE INDEX IF NOT EXISTS scroll_dispatch_outbox_due
                ON scroll_dispatch_outbox(peer_id, state, next_attempt_at);

             CREATE TABLE IF NOT EXISTS scroll_dispatch_inbox (
                sender_daemon_id TEXT NOT NULL,
                sender_seq       INTEGER NOT NULL,
                local_agent_id   TEXT NOT NULL,
                received_at      INTEGER NOT NULL,
                PRIMARY KEY (sender_daemon_id, sender_seq)
             );",
        )?;

        // One upserted row per agent: `base_commit` recorded at dispatch, the
        // rest at terminal state, so a restart's artifact reflects its last
        // run. `files_changed` is a JSON array of `FileChange`.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS agent_artifacts (
                agent_id      TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
                base_commit   TEXT,
                files_changed TEXT NOT NULL DEFAULT '[]',
                diff          TEXT,
                insertions    INTEGER NOT NULL DEFAULT 0,
                deletions     INTEGER NOT NULL DEFAULT 0,
                tokens_used   INTEGER NOT NULL DEFAULT 0,
                usd_spent     REAL NOT NULL DEFAULT 0,
                captured_at   INTEGER
            );",
        )?;

        // `agent.summon` idempotency keys: a key maps to the agent minted on
        // first use; repeats return it. Keys are global (collisions are the
        // caller's contract).
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS idempotency_keys (
                key        TEXT PRIMARY KEY,
                agent_id   TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );",
        )?;

        // Task retry policy + HITL approval gate. `max_retries`/`retry_count`
        // cap and track re-spawns; `requires_approval` holds a Ready task
        // until approved; `approval_state` is none/pending/approved/rejected.
        add_column_if_missing(
            &conn,
            "tasks",
            "max_retries",
            "max_retries INTEGER NOT NULL DEFAULT 0",
        )?;
        add_column_if_missing(
            &conn,
            "tasks",
            "retry_count",
            "retry_count INTEGER NOT NULL DEFAULT 0",
        )?;
        add_column_if_missing(
            &conn,
            "tasks",
            "requires_approval",
            "requires_approval INTEGER NOT NULL DEFAULT 0",
        )?;
        add_column_if_missing(
            &conn,
            "tasks",
            "approval_state",
            "approval_state TEXT NOT NULL DEFAULT 'none'",
        )?;

        // Agent lifecycle federation. Subscriptions are per-peer with no
        // per-agent wire filter — receivers filter via the
        // `RemoteAgentCompletion` wake source. Inbox dedupes on
        // `(sender_daemon_id, sender_seq)`.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS agent_lifecycle_federations (
                id           TEXT PRIMARY KEY,
                peer_id      TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                direction    TEXT NOT NULL,
                created_at   INTEGER NOT NULL,
                UNIQUE (peer_id)
             );
             CREATE TABLE IF NOT EXISTS agent_lifecycle_outbox (
                id              TEXT PRIMARY KEY,
                peer_id         TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                sender_seq      INTEGER NOT NULL,
                payload         BLOB NOT NULL,
                created_at      INTEGER NOT NULL,
                attempts        INTEGER NOT NULL DEFAULT 0,
                next_attempt_at INTEGER NOT NULL,
                state           TEXT NOT NULL
             );
             CREATE UNIQUE INDEX IF NOT EXISTS agent_lifecycle_outbox_seq
                ON agent_lifecycle_outbox(peer_id, sender_seq);
             CREATE INDEX IF NOT EXISTS agent_lifecycle_outbox_due
                ON agent_lifecycle_outbox(peer_id, state, next_attempt_at);
             CREATE TABLE IF NOT EXISTS agent_lifecycle_inbox (
                sender_daemon_id TEXT NOT NULL,
                sender_seq       INTEGER NOT NULL,
                received_at      INTEGER NOT NULL,
                PRIMARY KEY (sender_daemon_id, sender_seq)
             );",
        )?;

        Ok(())
    }
}
