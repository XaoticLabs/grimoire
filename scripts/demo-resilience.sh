#!/usr/bin/env bash
# The resilience demo: kill -9 the daemon mid-flight, restart it, and watch
# agents come back from the durable event log — supervised agents restart,
# the standing agent is still subscribed, and `grim chronicle` replays the
# whole life including the crash.
#
# Fully self-contained: runs in an isolated GRIMOIRE_DIR with a fake
# provider (a shell script), so it needs no API keys and spends no tokens.
# Record it with: asciinema rec -c scripts/demo-resilience.sh demo.cast
set -euo pipefail

GRIM="${GRIM:-$(command -v grim || echo target/release/grim)}"
[ -x "$GRIM" ] || { echo "grim binary not found (set GRIM=path)"; exit 1; }

DEMO_DIR="$(mktemp -d /tmp/grim-demo.XXXXXX)"
export GRIMOIRE_DIR="$DEMO_DIR/state"
WATCHED="$DEMO_DIR/repo"
mkdir -p "$GRIMOIRE_DIR" "$WATCHED"

cleanup() {
  [ -n "${DAEMON_PID:-}" ] && kill "$DAEMON_PID" 2>/dev/null || true
  pkill -f "$GRIMOIRE_DIR" 2>/dev/null || true
  rm -rf "$DEMO_DIR"
}
trap cleanup EXIT

say()  { printf '\n\033[1;35m▌ %s\033[0m\n' "$*"; }
run()  { printf '\033[1;34m$ %s\033[0m\n' "$*"; "$@"; }
pause(){ sleep "${1:-2}"; }

# A fake agent CLI: works slowly so we can kill the daemon mid-flight.
cat > "$DEMO_DIR/slow-worker.sh" <<'EOF'
#!/usr/bin/env bash
echo "starting on task: $1"
for i in $(seq 1 20); do echo "working… step $i/20"; sleep 1; done
echo "done."
EOF
chmod +x "$DEMO_DIR/slow-worker.sh"

# A fast one for the standing agent, so it parks in Dormant quickly.
cat > "$DEMO_DIR/sentinel.sh" <<'EOF'
#!/usr/bin/env bash
echo "sentinel ran with prompt:"; echo "$1" | head -3; echo "all quiet."
EOF
chmod +x "$DEMO_DIR/sentinel.sh"

cat > "$GRIMOIRE_DIR/config.toml" <<EOF
[providers.worker]
binary = "$DEMO_DIR/slow-worker.sh"
args_template = ["{task}"]

[providers.sentinel]
binary = "$DEMO_DIR/sentinel.sh"
args_template = ["{task}"]
EOF

say "1. Start the daemon (isolated state dir: \$GRIMOIRE_DIR)"
"$GRIM" daemon >"$DEMO_DIR/daemon.log" 2>&1 &
DAEMON_PID=$!
sleep 2
echo "daemon pid: $DAEMON_PID"

say "2. Summon two supervised agents (restart: on_failure) + one standing agent"
A1=$("$GRIM" summon --provider worker --restart on_failure --max-restarts 3/60s --name alpha "long task one" | awk '{print $3}')
A2=$("$GRIM" summon --provider worker --restart on_failure --max-restarts 3/60s --name beta  "long task two" | awk '{print $3}')
S1=$("$GRIM" summon --provider sentinel --keep-alive --cwd "$WATCHED" --name sentinel "watch this repo" | awk '{print $3}')
echo "alpha=$A1 beta=$A2 sentinel=$S1"
pause 4
run "$GRIM" wake add "$S1" --watch "**"
pause 2
run "$GRIM" circle

say "3. Mid-flight, kill the daemon the hard way"
run kill -9 "$DAEMON_PID"
pause 1
echo "(daemon is gone; nothing is coordinating)"

say "4. Restart the daemon — recovery comes from the event log"
"$GRIM" daemon >>"$DEMO_DIR/daemon.log" 2>&1 &
DAEMON_PID=$!
pause 4
run "$GRIM" circle
echo
echo "→ alpha/beta were mid-flight: marked failed by boot reconciliation,"
echo "  then restarted by their supervisor (policy on_failure)."
echo "→ sentinel is still Dormant, its file-watch survived the crash:"
run "$GRIM" wake list "$S1"

say "5. Wake the standing agent with a file change"
echo "change" >> "$WATCHED/file.txt"
pause 10
run "$GRIM" wake list "$S1"
run "$GRIM" chronicle "$S1" --no-output

say "6. Replay a life: every step, the crash, the restart — one timeline"
run "$GRIM" chronicle "$A1" --no-output
pause 1

say "That's the bet: agents are processes. kill -9 is survivable when the log is the source of truth."
