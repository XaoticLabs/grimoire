# Recipe: read a life, score it, fork the winner

The time-travel triad: `chronicle` reads what an agent did, `eval` scores
it, `fork` branches a new agent from any point in its past. Together they
turn "the agent went off the rails" from a shrug into a debuggable,
recoverable event.

Everything here works because the daemon writes every state change,
output line, wake, and mail through to a durable event log with
per-stream sequence numbers. The log is the agent's life; these three
commands are views over it.

## Read: where did it go wrong?

```bash
grim chronicle 4a                      # the full life, human-formatted
grim chronicle 4a --kinds state_change,wake_source_fired
grim chronicle 4a --until 120          # reconstruct state as of seq 120
grim chronicle 4a --json | jq ...      # scriptable
```

The footer of `--until` shows the agent's reconstructed state at that
point: session id, restart/wake counts, mail in and out. Find the seq
where the reasoning turned — that's your cut point.

## Score: was it actually bad?

Write a rubric (plain markdown, any criteria you want an evaluator to
apply):

```markdown
# rubric: did the migration stay in scope?
- Only files under db/migrations were modified: 0.4
- Each migration is reversible: 0.3
- No schema change lacks a covering test: 0.3
Return JSON: {"score": <0..1>, "reasons": ["..."]}
```

```bash
grim eval 4a --rubric rubric.md        # evaluator agent reads the transcript, scores it
grim eval 4a --list                    # stored verdicts
grim circle --eval-score-lt 0.7        # every agent scoring under 0.7
```

The evaluator is itself an agent — a separate context with no stake in
the work, which is exactly what self-review isn't.

## Fork: branch from the last good moment

```bash
grim fork 4a --at 119
grim fork 4a --at 119 --task "Same goal, but do NOT touch the ORM layer"
grim fork 4a --at 119 --provider pi    # same history, different brain
```

The fork is a new agent seeded with the parent's transcript up to the cut
as a provenance preamble. Parent untouched, fork independent, both fully
chronicled.

## Compose: fork and race

Fork the same cut point three ways with three strategies, let them run,
score all three with the same rubric, keep the winner:

```bash
for strategy in "use redis" "use postgres" "use sqlite"; do
  grim fork 4a --at 119 --task "Continue, choosing: $strategy"
done
grim eval <fork-1> --rubric rubric.md
grim eval <fork-2> --rubric rubric.md
grim eval <fork-3> --rubric rubric.md
grim circle --eval-score-gte 0.8       # the survivors
```

This is the loop that one-shot CLIs can't close: the transcript, the
judge, and the branch point all live in one place, owned by the daemon,
addressable after the fact.
