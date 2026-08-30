//! Typed argv, processes, pipelines, and I/O topology.

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use super::{Env, EnvPatch};

/// Non-empty, NUL-free argv. The first word is always the executable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Argv(Vec<String>);

impl Argv {
    pub fn new(
        program: impl Into<String>,
        arguments: impl IntoIterator<Item = String>,
    ) -> Result<Self, ProcessError> {
        let mut words = vec![program.into()];
        words.extend(arguments);
        Self::try_from(words)
    }

    pub fn program(&self) -> &str {
        &self.0[0]
    }

    pub fn arguments(&self) -> &[String] {
        &self.0[1..]
    }

    pub fn words(&self) -> &[String] {
        &self.0
    }

    pub fn into_words(self) -> Vec<String> {
        self.0
    }
}

impl TryFrom<Vec<String>> for Argv {
    type Error = ProcessError;

    fn try_from(words: Vec<String>) -> Result<Self, Self::Error> {
        let Some(program) = words.first() else {
            return Err(ProcessError::MissingProgram);
        };
        if program.is_empty() {
            return Err(ProcessError::EmptyProgram);
        }
        if let Some(index) = words.iter().position(|word| word.contains('\0')) {
            return Err(ProcessError::ArgumentContainsNul { index });
        }
        Ok(Self(words))
    }
}

impl<'de> Deserialize<'de> for Argv {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let words = Vec::<String>::deserialize(deserializer)?;
        Self::try_from(words).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Process {
    argv: Argv,
    #[serde(default, skip_serializing_if = "EnvPatch::is_empty")]
    env: EnvPatch,
}

impl Process {
    pub fn new(argv: Argv) -> Self {
        Self {
            argv,
            env: EnvPatch::empty(),
        }
    }

    pub fn with_env(argv: Argv, env: EnvPatch) -> Self {
        Self { argv, env }
    }

    pub fn argv(&self) -> &Argv {
        &self.argv
    }

    pub fn env(&self) -> &EnvPatch {
        &self.env
    }

    pub fn effective_env(&self, scope_env: &Env) -> Env {
        self.env.apply(scope_env)
    }
}

/// One structured process step. Its shape makes a missing or dangling pipe
/// link impossible to represent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pipeline {
    first: Process,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    rest: Vec<PipeContinuation>,
}

impl Pipeline {
    pub fn new(first: Process, rest: Vec<PipeContinuation>) -> Self {
        Self { first, rest }
    }

    pub fn simple(process: Process) -> Self {
        Self::new(process, Vec::new())
    }

    pub fn first(&self) -> &Process {
        &self.first
    }

    pub fn rest(&self) -> &[PipeContinuation] {
        &self.rest
    }

    pub fn processes(&self) -> impl Iterator<Item = &Process> {
        std::iter::once(&self.first).chain(self.rest.iter().map(|continuation| &continuation.next))
    }

    pub fn process_count(&self) -> usize {
        1 + self.rest.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipeContinuation {
    link: PipeLink,
    next: Process,
}

impl PipeContinuation {
    pub fn new(link: PipeLink, next: Process) -> Self {
        Self { link, next }
    }

    pub const fn link(&self) -> PipeLink {
        self.link
    }

    pub fn next(&self) -> &Process {
        &self.next
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipeLink {
    StdoutToStdin,
    StderrToStdin,
    StdoutAndStderrToStdin,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProcessError {
    #[error("argv must contain an executable")]
    MissingProgram,
    #[error("argv executable must not be empty")]
    EmptyProgram,
    #[error("argv word {index} contains NUL")]
    ArgumentContainsNul { index: usize },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{EnvEdit, EnvKey, EnvValue};

    fn key(value: &str) -> EnvKey {
        EnvKey::new(value).unwrap()
    }

    fn value(value: &str) -> EnvValue {
        EnvValue::new(value).unwrap()
    }

    fn argv(program: &str, arguments: &[&str]) -> Argv {
        Argv::new(program, arguments.iter().map(|value| (*value).to_owned())).unwrap()
    }

    #[test]
    fn argv_rejects_empty_and_nul_words_at_json_boundary() {
        assert!(serde_json::from_str::<Argv>("[]").is_err());
        assert!(serde_json::from_str::<Argv>(r#"[""]"#).is_err());
        assert!(serde_json::from_str::<Argv>(r#"["echo","a\u0000b"]"#).is_err());
    }

    #[test]
    fn process_environment_is_local_to_each_pipeline_process() {
        let scope = BTreeMap::from([(key("PATH"), value("/bin"))]);
        let left = Process::with_env(
            argv("printenv", &["MODE"]),
            EnvPatch::new(BTreeMap::from([(
                key("MODE"),
                EnvEdit::set("release").unwrap(),
            )])),
        );
        let right = Process::new(argv("wc", &["-l"]));
        let pipeline = Pipeline::new(
            left,
            vec![PipeContinuation::new(PipeLink::StdoutToStdin, right)],
        );

        let processes = pipeline.processes().collect::<Vec<_>>();
        assert_eq!(
            processes[0]
                .effective_env(&scope)
                .get(&key("MODE"))
                .unwrap()
                .as_str(),
            "release"
        );
        assert!(
            !processes[1]
                .effective_env(&scope)
                .contains_key(&key("MODE"))
        );
    }

    #[test]
    fn pipeline_serializes_as_first_plus_linked_continuations() {
        let pipeline = Pipeline::new(
            Process::new(argv("printf", &["hi"])),
            vec![PipeContinuation::new(
                PipeLink::StdoutToStdin,
                Process::new(argv("wc", &["-c"])),
            )],
        );
        let json = serde_json::to_value(pipeline).unwrap();
        assert!(json.get("first").is_some());
        assert_eq!(json["rest"].as_array().unwrap().len(), 1);
        assert!(json.get("processes").is_none());
        assert!(json.get("links").is_none());
    }
}
