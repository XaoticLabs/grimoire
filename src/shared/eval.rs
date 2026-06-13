//! Rubric-scored evaluation primitives shared by `grim eval` and the
//! daemon's verification-gated scroll tasks.
//!
//! The evaluator is prompted with [`build_eval_prompt`] (rubric + folded
//! transcript + `{"score": ...}` output spec); its reply is decoded with
//! [`parse_verdict`]. Keep prompt and parser in lock-step.

use anyhow::{Context, Result, anyhow};

use super::protocol::StreamEvent;

/// Max bytes of folded transcript sent to the evaluator. Tail-keeps the most
/// recent output and marks truncation explicitly. Deliberately mirrors `fork`'s
/// fold cap: 16 KiB is the line anywhere the daemon reasons about agent context.
pub const MAX_FOLDED_TRANSCRIPT: usize = 16 * 1024;

/// One rubric-scored verdict as emitted by an evaluator agent.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct EvalVerdict {
    /// Rubric score in `0.0..=1.0`; the only required field.
    pub score: f64,
    /// Coarse label, conventionally `pass` / `fail` / `partial`.
    #[serde(default)]
    pub verdict: Option<String>,
    /// Free-form one-paragraph justification for the score.
    #[serde(default)]
    pub rationale: Option<String>,
}

/// Build the evaluator prompt. Inputs are concatenated, not schema-templated,
/// so the rubric author can include anything — including JSON-output
/// instructions that override the default suggestion.
pub fn build_eval_prompt(target_id: &str, max_seq: i64, rubric: &str, transcript: &str) -> String {
    let (transcript_block, note) = if transcript.len() > MAX_FOLDED_TRANSCRIPT {
        let start = transcript.len() - MAX_FOLDED_TRANSCRIPT;
        (
            transcript[start..].to_string(),
            format!("[…earlier {start} bytes truncated]\n"),
        )
    } else {
        (transcript.to_string(), String::new())
    };

    format!(
        "You are an evaluator agent. Score the following agent's transcript \
         against the rubric. Apply the rubric strictly; do not invent criteria.\n\
         \n\
         === Rubric ===\n\
         {rubric}\n\
         \n\
         === Transcript (agent {target_id}, seq 0..={max_seq}) ===\n\
         {note}{transcript_block}\n\
         === End transcript ===\n\
         \n\
         === Output spec ===\n\
         Reply with a single JSON object on its own line, then exit. Schema:\n\
         {{\"score\": <number 0.0-1.0>, \"verdict\": \"pass\" | \"fail\" | \"partial\", \"rationale\": \"<one paragraph>\"}}\n\
         No commentary outside the JSON object."
    )
}

/// Pull the score JSON from the evaluator's reply, accepting a bare object or
/// one embedded anywhere — agents reliably drift from "JSON only" and rejecting
/// that drift would make eval brittle.
pub fn parse_verdict(text: &str) -> Result<EvalVerdict> {
    if let Ok(v) = serde_json::from_str::<EvalVerdict>(text.trim()) {
        return Ok(v);
    }
    let start = text
        .find('{')
        .ok_or_else(|| anyhow!("no JSON object in evaluator reply"))?;
    let end = text
        .rfind('}')
        .ok_or_else(|| anyhow!("unterminated JSON in evaluator reply"))?;
    if end <= start {
        return Err(anyhow!("malformed JSON range in evaluator reply"));
    }
    serde_json::from_str::<EvalVerdict>(&text[start..=end])
        .with_context(|| "parsing evaluator score JSON")
}

/// Fold stream events into evaluator transcript text: stdout `Output` lines
/// concatenated in order, everything else dropped. Shared by CLI and daemon.
pub fn fold_stdout_output<'a, I>(events: I) -> String
where
    I: IntoIterator<Item = &'a StreamEvent>,
{
    let mut buf = String::new();
    for event in events {
        if let StreamEvent::Output { stream, line, .. } = event
            && stream == "stdout"
        {
            buf.push_str(line);
            if !line.ends_with('\n') {
                buf.push('\n');
            }
        }
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eval_prompt_includes_rubric_transcript_and_schema() {
        let prompt = build_eval_prompt(
            "abc12345",
            7,
            "Did the agent identify the bug?",
            "agent said: there is a null deref on line 42\n",
        );
        assert!(prompt.contains("Did the agent identify the bug?"));
        assert!(prompt.contains("null deref on line 42"));
        assert!(prompt.contains("seq 0..=7"));
        assert!(prompt.contains("\"score\""));
        assert!(prompt.contains("\"verdict\""));
        assert!(prompt.contains("\"pass\""));
    }

    #[test]
    fn parse_verdict_accepts_bare_json() {
        let v = parse_verdict(r#"{"score":0.9,"verdict":"pass","rationale":"good"}"#).unwrap();
        assert!((v.score - 0.9).abs() < 1e-6);
        assert_eq!(v.verdict.as_deref(), Some("pass"));
    }

    #[test]
    fn parse_verdict_extracts_embedded_json() {
        let v = parse_verdict(
            "Here is my evaluation:\n{\"score\":0.4,\"verdict\":\"partial\"}\nThank you.",
        )
        .unwrap();
        assert!((v.score - 0.4).abs() < 1e-6);
        assert_eq!(v.verdict.as_deref(), Some("partial"));
    }

    #[test]
    fn parse_verdict_rejects_no_object() {
        assert!(parse_verdict("nothing useful here").is_err());
    }

    #[test]
    fn eval_prompt_tail_truncates_long_transcript() {
        let big = "x".repeat(MAX_FOLDED_TRANSCRIPT + 200);
        let prompt = build_eval_prompt("a", 1, "rubric", &big);
        assert!(prompt.contains("earlier"));
        assert!(prompt.contains("truncated"));
        // The output spec at the bottom must still survive truncation.
        assert!(prompt.contains("\"score\""));
    }

    #[test]
    fn fold_stdout_output_skips_stderr_and_other_events() {
        let events = [
            StreamEvent::Output {
                agent_id: "a".into(),
                stream: "stdout".into(),
                line: "keep me".into(),
            },
            StreamEvent::Output {
                agent_id: "a".into(),
                stream: "stderr".into(),
                line: "drop me".into(),
            },
        ];
        let folded = fold_stdout_output(events.iter());
        assert_eq!(folded, "keep me\n");
    }
}
