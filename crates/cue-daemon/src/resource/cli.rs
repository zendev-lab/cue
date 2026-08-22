//! JSON-over-stdio resource provider.
//!
//! This provider lets daemon operators add arbitrary resource classes without
//! Rust changes. Each provider owns a configured set of `need.<key>` names and
//! delegates probe/reserve/release to direct-exec argv commands.
//!
//! Protocol:
//! * `probe` stdin: empty; stdout: `{ "units": [{"id":"...","attrs":{...}}] }`
//! * `reserve` stdin: `{ "step_id":"E1/S1", "needs": {...} }`; stdout either
//!   `{ "ok": true, "grant_id":"...", "env": {...}, "info": {...} }` or
//!   `{ "ok": false, "reason":"..." }`
//! * `release` stdin: `{ "grant_id":"..." }`; stdout ignored.

use std::{
    collections::BTreeMap,
    io::Write,
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use tracing::warn;

use cue_core::{
    StepId,
    resource::{
        Grant, Need, ProviderId, Reject, Reservation, ReservationId, ResourceQuantity,
        ResourceUnit, Snapshot,
    },
};

use crate::config::CliResourceProviderConfig;

use super::provider::{Provider, ReserveRequest};

#[derive(Debug, Clone)]
struct CliCommand {
    argv: Vec<String>,
}

impl CliCommand {
    fn new(argv: Vec<String>) -> Self {
        Self { argv }
    }

    fn display(&self) -> String {
        self.argv.join(" ")
    }
}

#[derive(Debug)]
pub struct CliProvider {
    id: ProviderId,
    keys: Vec<String>,
    probe: CliCommand,
    reserve: CliCommand,
    release: CliCommand,
    timeout: Duration,
}

impl CliProvider {
    pub fn from_config(id: &str, config: &CliResourceProviderConfig) -> Self {
        Self {
            id: ProviderId::new(id),
            keys: config.keys.clone(),
            probe: CliCommand::new(config.probe.clone()),
            reserve: CliCommand::new(config.reserve.clone()),
            release: CliCommand::new(config.release.clone()),
            timeout: Duration::from_millis(config.timeout_ms),
        }
    }

    #[cfg(test)]
    fn new_for_test(
        id: &str,
        keys: &[&str],
        probe: Vec<String>,
        reserve: Vec<String>,
        release: Vec<String>,
    ) -> Self {
        Self {
            id: ProviderId::new(id),
            keys: keys.iter().map(|key| (*key).to_string()).collect(),
            probe: CliCommand::new(probe),
            reserve: CliCommand::new(reserve),
            release: CliCommand::new(release),
            timeout: Duration::from_secs(5),
        }
    }

    fn run(&self, command: &CliCommand, stdin: Option<&[u8]>) -> Result<Output> {
        let program = command
            .argv
            .first()
            .ok_or_else(|| anyhow!("empty resource provider command"))?;
        let mut child = Command::new(program)
            .args(&command.argv[1..])
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("spawn `{}`", command.display()))?;

        if let Some(input) = stdin {
            let mut child_stdin = child
                .stdin
                .take()
                .ok_or_else(|| anyhow!("stdin pipe unavailable for `{}`", command.display()))?;
            child_stdin
                .write_all(input)
                .with_context(|| format!("write stdin to `{}`", command.display()))?;
        }

        let deadline = Instant::now() + self.timeout;
        loop {
            if child
                .try_wait()
                .with_context(|| format!("poll `{}`", command.display()))?
                .is_some()
            {
                return child
                    .wait_with_output()
                    .with_context(|| format!("collect `{}` output", command.display()));
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(anyhow!(
                    "`{}` timed out after {}ms",
                    command.display(),
                    self.timeout.as_millis()
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn run_json<T: for<'de> Deserialize<'de>>(
        &self,
        command: &CliCommand,
        stdin: Option<&[u8]>,
    ) -> Result<T> {
        let output = self.run(command, stdin)?;
        if !output.status.success() {
            return Err(anyhow!(
                "`{}` exited with status {}{}{}",
                command.display(),
                output.status,
                format_output_suffix("stderr", &output.stderr),
                format_output_suffix("stdout", &output.stdout)
            ));
        }
        serde_json::from_slice::<T>(&output.stdout)
            .with_context(|| format!("parse `{}` stdout as JSON", command.display()))
    }
}

impl Provider for CliProvider {
    fn id(&self) -> ProviderId {
        self.id.clone()
    }

    fn keys(&self) -> Vec<String> {
        self.keys.clone()
    }

    fn probe(&self) -> Snapshot {
        match self.run_json::<ProbeResponse>(&self.probe, None) {
            Ok(response) => Snapshot::new(self.id.clone(), response.units),
            Err(error) => {
                warn!(provider = %self.id, "resource CLI probe failed: {error:#}");
                Snapshot::new(self.id.clone(), Vec::new())
            }
        }
    }

    fn reserve(&self, req: &ReserveRequest) -> Result<Grant, Reject> {
        let request = ReserveRequestPayload {
            step_id: req.step_id.to_string(),
            needs: &req.need,
        };
        let stdin = serde_json::to_vec(&request)
            .map_err(|error| Reject::new(format!("encode reserve request: {error}")))?;
        let response = self
            .run_json::<ReserveResponse>(&self.reserve, Some(&stdin))
            .map_err(|error| {
                Reject::new(format!(
                    "CLI provider {} reserve failed: {error:#}",
                    self.id
                ))
            })?;

        if !response.ok {
            return Err(Reject::new(response.reason.unwrap_or_else(|| {
                format!("CLI provider {} rejected reservation", self.id)
            })));
        }

        let grant_id = response.grant_id.ok_or_else(|| {
            Reject::new(format!(
                "CLI provider {} returned ok=true without grant_id",
                self.id
            ))
        })?;
        Ok(Reservation {
            id: ReservationId::new(grant_id),
            step_id: req.step_id,
            provider_id: self.id.clone(),
            env: response.env,
            info: response.info,
            acquired_at: std::time::SystemTime::now(),
        })
    }

    fn release(&self, grant_id: &ReservationId) {
        let request = ReleaseRequestPayload {
            grant_id: grant_id.to_string(),
        };
        match serde_json::to_vec(&request)
            .map_err(anyhow::Error::from)
            .and_then(|stdin| self.run(&self.release, Some(&stdin)).map(|_| ()))
        {
            Ok(()) => {}
            Err(error) => {
                warn!(provider = %self.id, %grant_id, "resource CLI release failed: {error:#}")
            }
        }
    }
}

fn format_output_suffix(label: &str, bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let text = text.trim();
    if text.is_empty() {
        String::new()
    } else {
        format!("\n{label}: {text}")
    }
}

#[derive(Debug, Deserialize)]
struct ProbeResponse {
    #[serde(default)]
    units: Vec<ResourceUnit>,
}

#[derive(Debug, Serialize)]
struct ReserveRequestPayload<'a> {
    step_id: String,
    needs: &'a Need,
}

#[derive(Debug, Deserialize)]
struct ReserveResponse {
    ok: bool,
    #[serde(default)]
    grant_id: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    info: BTreeMap<String, ResourceQuantity>,
}

#[derive(Debug, Serialize)]
struct ReleaseRequestPayload {
    grant_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use cue_core::resource::ResourceQuantity;

    fn fixture(name: &str, command: &str) -> Vec<String> {
        vec![
            format!(
                "{}/tests/fixtures/cli-provider/{name}",
                env!("CARGO_MANIFEST_DIR")
            ),
            command.to_string(),
        ]
    }

    #[test]
    fn probe_parses_units() {
        let provider = CliProvider::new_for_test(
            "licenses",
            &["license"],
            fixture("success.sh", "probe"),
            fixture("success.sh", "reserve"),
            fixture("success.sh", "release"),
        );

        let snapshot = provider.probe();
        assert_eq!(snapshot.provider_id, ProviderId::new("licenses"));
        assert_eq!(snapshot.units.len(), 1);
        assert_eq!(snapshot.units[0].id, "pool");
        assert_eq!(
            snapshot.units[0].attrs.get("free"),
            Some(&ResourceQuantity::Count(3))
        );
    }

    #[test]
    fn reserve_round_trips_need_and_builds_grant() {
        let provider = CliProvider::new_for_test(
            "licenses",
            &["license"],
            fixture("success.sh", "probe"),
            fixture("success.sh", "reserve"),
            fixture("success.sh", "release"),
        );

        let need = Need::from_pairs([("license", ResourceQuantity::Count(1))]);
        let grant = provider
            .reserve(&ReserveRequest::new(
                StepId {
                    execution: cue_core::ExecutionId(7),
                    index: 1,
                },
                need,
            ))
            .expect("grant");
        assert_eq!(grant.id, ReservationId::new("g1"));
        assert_eq!(
            grant.env.get("LICENSE_TOKEN").map(String::as_str),
            Some("abc")
        );
        assert_eq!(grant.info.get("license"), Some(&ResourceQuantity::Count(1)));
    }

    #[test]
    fn reserve_rejects_ok_without_grant_id() {
        let provider = CliProvider::new_for_test(
            "licenses",
            &["license"],
            fixture("missing-grant.sh", "probe"),
            fixture("missing-grant.sh", "reserve"),
            fixture("missing-grant.sh", "release"),
        );

        let err = provider
            .reserve(&ReserveRequest::new(
                StepId {
                    execution: cue_core::ExecutionId(1),
                    index: 1,
                },
                Need::from_pairs([("license", ResourceQuantity::Count(1))]),
            ))
            .expect_err("missing grant_id should reject");
        assert!(err.reason.contains("without grant_id"));
    }
}
