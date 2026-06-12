# Agents are processes, not function calls

> *The case for giving an AI agent a pid, a mailbox, and a supervisor — and what becomes possible once you do.*

Every agent framework I've used shares one architectural assumption so universal it's invisible: the agent is a function call. Your program invokes it, blocks (or awaits), gets a result, and the agent ceases to exist. LangGraph graphs, CrewAI crews, the while-loop around `claude -p` in your deploy script — different ergonomics, same lifecycle. The agent lives exactly as long as the caller's stack frame.

That assumption was fine when agents ran for thirty seconds. It's collapsing now, for a boring operational reason: the interesting agent workloads no longer fit inside a caller's lifetime. A reviewer that watches a repo for weeks. An auditor that runs at 3am. A triage agent that wakes when Sentry posts. The moment the work outlives the invocation, "agent as function call" stops being an architecture and becomes a bug — your orchestrator dies when your laptop lid closes, and everything it knew dies with it.

The fix is old. We solved this for ordinary software decades ago and named the solution: **processes**. Identity that persists. A supervisor that restarts you. An address where others can reach you. State that survives a crash because it lives somewhere durable, not in the caller's memory. [Grimoire](https://github.com/XaoticLabs/grimoire) is an experiment in applying that, literally, to AI coding agents: a Rust daemon — `cron` + `systemd` for agents, bring your own CLI — where an agent you summon today still has an address next week.

## Two primitives

Strip the project to its load-bearing walls and there are exactly two.

**A durable event log.** Every state change, output line, wake, restart, and message is written through to SQLite with per-stream sequence numbers before anyone consumes it. Not logging — the log *is* the system's memory. On boot, the daemon reconciles reality against it: agents that were mid-flight when the machine died are marked failed, their supervisors evaluate restart policies, dormant agents rehydrate their wake triggers. Nothing is lost because nothing important ever existed only in RAM.

**An addressable mailbox.** Every agent is `agent://<id>`; topics fan out to subscribers. Delivery to a sleeping agent *is* the wake mechanism.

Everything else in the system is composition. A cron wake is mail the scheduler sends. A file-watch wake is mail the workspace watcher sends. A webhook is mail an HTTP handler sends. Supervision escalation is mail to the parent. Federation is mail between daemons over mTLS. One delivery path, exhaustively reused — which means one path to test, one path that has to be right.

## What you get for the price of a daemon

Each of these is awkward-to-impossible in a library, and falls out naturally here:

**Standing agents.** `grim summon --keep-alive` parks an agent in `Dormant` after its first run. It wakes on cron, file change, mail, webhook, or another agent finishing — and because the daemon owns the wake, this works even though no agent "calls back." When it wakes, it resumes its own session (natively for CLIs with session models, by transcript replay from the event log for everything else), so it remembers Tuesday's flaky test on Thursday.

**Supervision.** Restart policies with budgets, borrowed from Erlang/OTP. The genuinely new part: when a child exhausts its restart budget, escalation routes to the *parent agent's mailbox*, and the recovery logic is whatever the parent model decides when it reads the failure. The supervisor doesn't need to know what to do; it needs to know who to wake.

**Time travel.** Because the log is total, `grim chronicle <id>` replays any agent's full life; `grim fork <id> --at <seq>` branches a new agent from any moment in the parent's past; `grim eval <id> --rubric <file>` has a disinterested agent score the transcript. Read a life, score it, fork the winner — debugging an agent stops being archaeology.

**A fabric, not a box.** Two daemons peer over mutually-authenticated gRPC: mail, topics, a replicated KV namespace, workspace file events, and scroll tasks all cross the wire. A file change on your laptop can wake an agent on the build server. A worker pool (`grimw`) puts the same control plane over many machines.

## The honest counterargument

"Multi-agent swarms are usually worse than one capable agent." Agreed — and that's not what this is. Grimoire's bet isn't that twenty agents talking to each other beat one good one; most of the time they don't. The bet is about *lifecycle*: even a single capable agent needs identity, durability, scheduling, supervision, and an audit trail the moment it runs longer than your attention span. That's not a framework problem. It's an init-system problem, and bolting it onto every agent framework separately is how we get twenty bad init systems.

There's also a quieter argument for the supervisor being a separate, neutral layer: the agent CLI you love this year may not be the one you love next year, and this year already demonstrated that vendor terms can change under you mid-flight. Grimoire deliberately knows almost nothing about the CLIs it runs — they're processes that take a prompt and print. The supervisor outlives any one vendor's harness, the event log is yours, and switching brains is a config line.

## Where it stands

Everything above is built and on `main`: the log, standing agents, supervision trees, mailboxes and topics, workspaces with shared memory, sandboxing (bwrap jail + cgroup limits), budgets and policy gates, chronicle/fork/eval, Prometheus metrics, the worker pool, and federation across daemons. ~45k lines of Rust, ~800 tests, including two-daemon federation tests over real mTLS gRPC.

The fastest way to feel the difference is the loop one-shot CLIs can't do:

```bash
grim demo standing-review --repo . --provider claude
```

A reviewer is now asleep in your repo. Edit a file: it wakes, diffs, decides, pings you if a human should look, and goes back to sleep. Kill the daemon with `-9` and start it again: the reviewer is still there, still subscribed, still remembers what it saw — because it's a process, not a function call.
