use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::{ExecutionId, ScopeHash, StepId};

use super::{EnvPatch, FileModeMask, Pipeline};

const MAX_PLAN_DEPTH: usize = 1_024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct CdPath(PathBuf);

impl CdPath {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, PlanValidationError> {
        let path = path.into();
        if path.as_os_str().is_empty() {
            return Err(PlanValidationError::EmptyCdPath);
        }
        if path.as_os_str().as_encoded_bytes().contains(&0) {
            return Err(PlanValidationError::CdPathContainsNul);
        }
        Ok(Self(path))
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }

    pub fn into_path_buf(self) -> PathBuf {
        self.0
    }
}

impl<'de> Deserialize<'de> for CdPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let path = PathBuf::deserialize(deserializer)?;
        Self::new(path).map_err(serde::de::Error::custom)
    }
}

/// A non-empty environment mutation used by the Env builtin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct EnvMutation(EnvPatch);

impl EnvMutation {
    pub fn new(patch: EnvPatch) -> Result<Self, PlanValidationError> {
        if patch.is_empty() {
            return Err(PlanValidationError::EmptyEnvMutation);
        }
        Ok(Self(patch))
    }

    pub fn patch(&self) -> &EnvPatch {
        &self.0
    }

    pub fn into_patch(self) -> EnvPatch {
        self.0
    }
}

impl<'de> Deserialize<'de> for EnvMutation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let patch = EnvPatch::deserialize(deserializer)?;
        Self::new(patch).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum BuiltinCommand {
    Cd(CdPath),
    Env(EnvMutation),
    Umask(FileModeMask),
}

impl BuiltinCommand {
    pub fn cd(path: impl Into<PathBuf>) -> Result<Self, PlanValidationError> {
        Ok(Self::Cd(CdPath::new(path)?))
    }

    pub fn env(patch: EnvPatch) -> Result<Self, PlanValidationError> {
        Ok(Self::Env(EnvMutation::new(patch)?))
    }

    pub const fn umask(mask: FileModeMask) -> Self {
        Self::Umask(mask)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IoMode {
    Captured,
    Pty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SequenceCondition {
    Success,
    Failure,
    Always,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParallelJoin {
    All,
    AnySuccess,
}

/// Parallel composition with at least two branches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ParallelBranches(Vec<ExecutionPlan>);

impl ParallelBranches {
    pub fn new(branches: Vec<ExecutionPlan>) -> Result<Self, PlanValidationError> {
        if branches.len() < 2 {
            return Err(PlanValidationError::TooFewParallelBranches {
                actual: branches.len(),
            });
        }
        Ok(Self(branches))
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &ExecutionPlan> + ExactSizeIterator {
        self.0.iter()
    }

    pub fn as_slice(&self) -> &[ExecutionPlan] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<ExecutionPlan> {
        self.0
    }
}

impl<'de> Deserialize<'de> for ParallelBranches {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let branches = Vec::<ExecutionPlan>::deserialize(deserializer)?;
        Self::new(branches).map_err(serde::de::Error::custom)
    }
}

/// Closed, finite execution semantics. Extensions may change how a leaf is
/// realized, but cannot add variants to this algebra.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionPlan {
    Builtin {
        command: BuiltinCommand,
    },
    Run {
        pipeline: Pipeline,
        io: IoMode,
    },
    Sequence {
        first: Box<Self>,
        then: Box<Self>,
        when: SequenceCondition,
    },
    Parallel {
        branches: ParallelBranches,
        join: ParallelJoin,
    },
}

impl ExecutionPlan {
    pub fn builtin(command: BuiltinCommand) -> Self {
        Self::Builtin { command }
    }

    pub fn run(pipeline: Pipeline, io: IoMode) -> Self {
        Self::Run { pipeline, io }
    }

    pub fn sequence(first: Self, then: Self, when: SequenceCondition) -> Self {
        Self::Sequence {
            first: Box::new(first),
            then: Box::new(then),
            when,
        }
    }

    pub fn parallel(branches: Vec<Self>, join: ParallelJoin) -> Result<Self, PlanValidationError> {
        Ok(Self::Parallel {
            branches: ParallelBranches::new(branches)?,
            join,
        })
    }

    pub fn validate(&self) -> Result<(), PlanValidationError> {
        let mut stack = vec![(self, 1usize)];
        let mut leaves = 0u64;
        while let Some((plan, depth)) = stack.pop() {
            if depth > MAX_PLAN_DEPTH {
                return Err(PlanValidationError::PlanTooDeep {
                    maximum: MAX_PLAN_DEPTH,
                });
            }
            match plan {
                Self::Builtin { .. } | Self::Run { .. } => {
                    leaves += 1;
                    if leaves > u32::MAX as u64 {
                        return Err(PlanValidationError::TooManySteps);
                    }
                }
                Self::Sequence { first, then, .. } => {
                    stack.push((then, depth + 1));
                    stack.push((first, depth + 1));
                }
                Self::Parallel { branches, .. } => {
                    for branch in branches.iter().rev() {
                        stack.push((branch, depth + 1));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn leaf_count(&self) -> Result<u32, PlanValidationError> {
        self.validate()?;
        let mut leaves = 0u32;
        let mut stack = vec![self];
        while let Some(plan) = stack.pop() {
            match plan {
                Self::Builtin { .. } | Self::Run { .. } => leaves += 1,
                Self::Sequence { first, then, .. } => {
                    stack.push(then);
                    stack.push(first);
                }
                Self::Parallel { branches, .. } => {
                    stack.extend(branches.iter());
                }
            }
        }
        Ok(leaves)
    }

    pub fn steps(
        &self,
        execution: ExecutionId,
    ) -> Result<Vec<StepDescriptor>, PlanValidationError> {
        self.validate()?;
        let mut steps = Vec::new();
        let mut stack = vec![self];
        while let Some(plan) = stack.pop() {
            match plan {
                Self::Builtin { .. } => steps.push(StepDescriptor {
                    id: StepId {
                        execution,
                        index: (steps.len() + 1) as u32,
                    },
                    kind: StepKind::Builtin,
                }),
                Self::Run { pipeline, io } => steps.push(StepDescriptor {
                    id: StepId {
                        execution,
                        index: (steps.len() + 1) as u32,
                    },
                    kind: StepKind::Run {
                        io: *io,
                        processes: pipeline.process_count(),
                    },
                }),
                Self::Sequence { first, then, .. } => {
                    stack.push(then);
                    stack.push(first);
                }
                Self::Parallel { branches, .. } => {
                    for branch in branches.iter().rev() {
                        stack.push(branch);
                    }
                }
            }
        }
        Ok(steps)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSpec {
    scope: ScopeHash,
    plan: ExecutionPlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_of: Option<ExecutionId>,
}

impl<'de> Deserialize<'de> for ExecutionSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireExecutionSpec {
            scope: ScopeHash,
            plan: ExecutionPlan,
            #[serde(default)]
            retry_of: Option<ExecutionId>,
        }

        let wire = WireExecutionSpec::deserialize(deserializer)?;
        let mut spec = Self::new(wire.scope, wire.plan).map_err(serde::de::Error::custom)?;
        spec.retry_of = wire.retry_of;
        Ok(spec)
    }
}

impl ExecutionSpec {
    pub fn new(scope: ScopeHash, plan: ExecutionPlan) -> Result<Self, PlanValidationError> {
        plan.validate()?;
        Ok(Self {
            scope,
            plan,
            retry_of: None,
        })
    }

    pub fn with_retry_of(mut self, execution: ExecutionId) -> Self {
        self.retry_of = Some(execution);
        self
    }

    pub const fn scope(&self) -> ScopeHash {
        self.scope
    }

    pub fn plan(&self) -> &ExecutionPlan {
        &self.plan
    }

    pub const fn retry_of(&self) -> Option<ExecutionId> {
        self.retry_of
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepDescriptor {
    pub id: StepId,
    pub kind: StepKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepKind {
    Builtin,
    Run { io: IoMode, processes: usize },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PlanValidationError {
    #[error("cd path must not be empty")]
    EmptyCdPath,
    #[error("cd path contains NUL")]
    CdPathContainsNul,
    #[error("Env builtin must contain at least one edit")]
    EmptyEnvMutation,
    #[error("parallel composition requires at least two branches, got {actual}")]
    TooFewParallelBranches { actual: usize },
    #[error("execution plan exceeds maximum depth {maximum}")]
    PlanTooDeep { maximum: usize },
    #[error("execution plan contains more steps than StepId can represent")]
    TooManySteps,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::vnext::{Argv, EnvEdit, EnvKey, PipeContinuation, PipeLink, Process};

    fn process(program: &str) -> Process {
        Process::new(Argv::new(program, Vec::new()).unwrap())
    }

    fn run(program: &str, io: IoMode) -> ExecutionPlan {
        ExecutionPlan::run(Pipeline::simple(process(program)), io)
    }

    #[test]
    fn builtin_env_and_parallel_cardinality_are_structurally_validated() {
        assert!(BuiltinCommand::env(EnvPatch::empty()).is_err());
        assert!(serde_json::from_str::<EnvMutation>("{}").is_err());
        assert!(ParallelBranches::new(vec![run("one", IoMode::Captured)]).is_err());

        let encoded = serde_json::to_string(&vec![run("one", IoMode::Captured)]).unwrap();
        assert!(serde_json::from_str::<ParallelBranches>(&encoded).is_err());
    }

    #[test]
    fn every_builtin_and_run_leaf_receives_a_stable_step_id() {
        let env = EnvPatch::new(BTreeMap::from([(
            EnvKey::new("MODE").unwrap(),
            EnvEdit::set("release").unwrap(),
        )]));
        let first = ExecutionPlan::builtin(BuiltinCommand::env(env).unwrap());
        let pipeline = Pipeline::new(
            process("printf"),
            vec![PipeContinuation::new(
                PipeLink::StdoutToStdin,
                process("wc"),
            )],
        );
        let second = ExecutionPlan::run(pipeline, IoMode::Pty);
        let third = ExecutionPlan::builtin(BuiltinCommand::cd("repo").unwrap());
        let plan = ExecutionPlan::sequence(
            first,
            ExecutionPlan::parallel(vec![second, third], ParallelJoin::All).unwrap(),
            SequenceCondition::Success,
        );

        let steps = plan.steps(ExecutionId(9)).unwrap();
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0].id.index, 1);
        assert_eq!(steps[1].id.index, 2);
        assert_eq!(steps[2].id.index, 3);
        assert_eq!(steps[0].kind, StepKind::Builtin);
        assert_eq!(
            steps[1].kind,
            StepKind::Run {
                io: IoMode::Pty,
                processes: 2,
            }
        );
        assert_eq!(steps[2].kind, StepKind::Builtin);
    }

    #[test]
    fn execution_spec_requires_an_explicit_scope_and_has_no_launch_context() {
        let spec = ExecutionSpec::new(ScopeHash([7; 32]), run("echo", IoMode::Captured))
            .unwrap()
            .with_retry_of(ExecutionId(4));
        let json = serde_json::to_value(&spec).unwrap();
        assert!(json.get("scope").is_some());
        assert!(json.get("start_scope").is_none());
        assert!(json.get("launch_context").is_none());
        assert_eq!(spec.retry_of(), Some(ExecutionId(4)));

        let without_scope = serde_json::json!({ "plan": json["plan"].clone() });
        assert!(serde_json::from_value::<ExecutionSpec>(without_scope).is_err());
    }

    #[test]
    fn plan_json_has_only_the_closed_four_variants() {
        let plans = [
            ExecutionPlan::builtin(BuiltinCommand::cd("repo").unwrap()),
            run("echo", IoMode::Captured),
            ExecutionPlan::sequence(
                run("a", IoMode::Captured),
                run("b", IoMode::Captured),
                SequenceCondition::Always,
            ),
            ExecutionPlan::parallel(
                vec![run("a", IoMode::Captured), run("b", IoMode::Captured)],
                ParallelJoin::AnySuccess,
            )
            .unwrap(),
        ];
        let kinds = plans
            .iter()
            .map(|plan| {
                serde_json::to_value(plan).unwrap()["kind"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(kinds, ["builtin", "run", "sequence", "parallel"]);
    }
}
