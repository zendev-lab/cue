//! Typed execution plans and their pure state reducer.
//!
//! An [`Execution`] is the sole lifecycle owner for one submitted plan. The
//! daemon may project and persist its state, but orchestration decisions stay
//! here so pipelines, conditions, parallel branches, and context changes do
//! not grow independent state machines.

use std::{error::Error, fmt, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    id::{ExecutionId, ScopeHash, StepId},
    job::SandboxSettings,
    pipeline::Pipeline,
    resource::Need,
    scope::EnvDelta,
};

/// A typed, tree-shaped execution plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionPlan {
    Pipeline { pipeline: Pipeline },
    OnSuccess { left: Box<Self>, right: Box<Self> },
    OnFailure { left: Box<Self>, right: Box<Self> },
    Always { left: Box<Self>, right: Box<Self> },
    ParallelAll { branches: Vec<Self> },
    AnySuccess { branches: Vec<Self> },
    ContextDelta { delta: EnvDelta },
}

/// Complete typed submission contract, independent of any frontend language.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSpec {
    pub plan: ExecutionPlan,
    /// Explicit starting scope. `None` asks the daemon to use the session cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_scope: Option<ScopeHash>,
    #[serde(default)]
    pub launch_context: LaunchContext,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceMetadata>,
    /// A retry always creates a new execution and points back to the old one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_of: Option<ExecutionId>,
}

/// Non-language launch controls shared by every process leaf in an execution.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchContext {
    /// Explicit PTY setting. `None` means use the session/config default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pty: Option<bool>,
    #[serde(default, skip_serializing_if = "Need::is_empty")]
    pub needs: Need,
    /// Cue's optional overlay view. This is not an external policy sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_view: Option<SandboxSettings>,
    /// Optional per-submission override for the configured process wrapper.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wrapper_enabled: Option<bool>,
    /// Ephemeral per-execution spawn interception lease. Never persisted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spawn_adapter: Option<SpawnAdapterHandle>,
}

/// Ephemeral local handle used to intercept process launch and settlement.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpawnAdapterHandle {
    pub endpoint: PathBuf,
    pub token: String,
}

impl fmt::Debug for SpawnAdapterHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpawnAdapterHandle")
            .field("endpoint", &self.endpoint)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

/// Optional compiler-provided source location for diagnostics and display.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceMetadata {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<u32>,
}

/// Stable DFS identity for every executable plan leaf, including context deltas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlanNodeId(pub u32);

/// Work the coordinator must perform for a ready plan node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionNode {
    pub id: PlanNodeId,
    /// Only process-bearing pipeline nodes receive public Step IDs.
    pub step_id: Option<StepId>,
    pub action: ExecutionAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionAction {
    Pipeline(Pipeline),
    ContextDelta(EnvDelta),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum StepState {
    Queued,
    Running,
    Succeeded,
    Failed { failure: StepFailure },
    Cancelled { reason: StepCancelReason },
}

impl StepState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed { .. } | Self::Cancelled { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StepFailure {
    Exit { code: i32 },
    Signal { signal: i32 },
    Spawn { message: String },
    Infrastructure { message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepCancelReason {
    User,
    Forced,
    ConditionNotMet,
    AnySuccessSatisfied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ExecutionState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled { reason: ExecutionCancelReason },
}

impl ExecutionState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled { .. }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionCancelReason {
    User,
    Forced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelMode {
    Graceful,
    Force,
}

/// Result supplied by the coordinator when one ready node finishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeOutcome {
    Succeeded,
    Failed(StepFailure),
}

/// Work exposed by one reducer transition.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutionTransition {
    pub newly_ready: Vec<PlanNodeId>,
    /// Running process steps which the coordinator must terminate.
    pub to_cancel: Vec<StepId>,
}

/// The sole mutable lifecycle state for one execution plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Execution {
    id: ExecutionId,
    spec: ExecutionSpec,
    /// DFS leaf order is derived from `spec.plan`; only leaf states are mutable.
    node_states: Vec<StepState>,
    cancelled: Option<ExecutionCancelReason>,
}

impl Execution {
    pub fn new(id: ExecutionId, spec: ExecutionSpec) -> Result<Self, PlanValidationError> {
        spec.plan.validate()?;
        let node_states = vec![StepState::Queued; spec.plan.node_count()];
        Ok(Self {
            id,
            spec,
            node_states,
            cancelled: None,
        })
    }

    pub fn id(&self) -> ExecutionId {
        self.id
    }

    pub fn spec(&self) -> &ExecutionSpec {
        &self.spec
    }

    pub fn state(&self) -> ExecutionState {
        if let Some(reason) = self.cancelled {
            return ExecutionState::Cancelled { reason };
        }

        // Drive a projection so aggregate status and readiness share exactly
        // one implementation of condition and parallel semantics.
        let mut projected_states = self.node_states.clone();
        let mut ignored_transition = ExecutionTransition::default();
        let step_ids = vec![None; projected_states.len()];
        match drive(
            &self.spec.plan,
            0,
            &step_ids,
            &mut projected_states,
            &mut ignored_transition,
        )
        .1
        {
            SubtreeState::Succeeded => ExecutionState::Succeeded,
            SubtreeState::Failed => ExecutionState::Failed,
            SubtreeState::Cancelled => ExecutionState::Cancelled {
                reason: ExecutionCancelReason::User,
            },
            SubtreeState::Waiting => {
                if self
                    .node_states
                    .iter()
                    .any(|state| matches!(state, StepState::Running))
                {
                    ExecutionState::Running
                } else {
                    ExecutionState::Queued
                }
            }
        }
    }

    /// Return the immutable work descriptor derived from the authoritative plan.
    pub fn node(&self, id: PlanNodeId) -> Option<ExecutionNode> {
        let mut nodes = Vec::with_capacity(self.node_states.len());
        collect_nodes(&self.spec.plan, self.id, &mut 1, &mut nodes);
        nodes.into_iter().find(|node| node.id == id)
    }

    pub fn nodes(&self) -> Vec<ExecutionNode> {
        let mut nodes = Vec::with_capacity(self.node_states.len());
        collect_nodes(&self.spec.plan, self.id, &mut 1, &mut nodes);
        nodes
    }

    pub fn step_state(&self, id: StepId) -> Option<&StepState> {
        if id.execution != self.id || id.index == 0 {
            return None;
        }
        let node = self
            .nodes()
            .into_iter()
            .find(|node| node.step_id == Some(id))?;
        self.node_states.get(node_index(node.id))
    }

    pub fn node_state(&self, id: PlanNodeId) -> Option<&StepState> {
        self.node_states.get(node_index(id))
    }

    /// Compute ready work and apply deterministic condition/race cancellations.
    pub fn advance(&mut self) -> ExecutionTransition {
        if self.cancelled.is_some() {
            return ExecutionTransition::default();
        }

        let mut transition = ExecutionTransition::default();
        let step_ids = self
            .nodes()
            .into_iter()
            .map(|node| node.step_id)
            .collect::<Vec<_>>();
        drive(
            &self.spec.plan,
            0,
            &step_ids,
            &mut self.node_states,
            &mut transition,
        );
        transition.newly_ready.retain(|id| {
            matches!(
                self.node_states.get(node_index(*id)),
                Some(StepState::Queued)
            )
        });
        transition.newly_ready.sort_unstable();
        transition.newly_ready.dedup();
        transition.to_cancel.sort_unstable();
        transition.to_cancel.dedup();
        transition
    }

    pub fn mark_running(&mut self, id: PlanNodeId) -> Result<(), TransitionError> {
        let ready = self.advance().newly_ready;
        if !ready.contains(&id) {
            return Err(TransitionError::NotReady(id));
        }
        let state = self
            .node_states
            .get_mut(node_index(id))
            .ok_or(TransitionError::UnknownNode(id))?;
        *state = StepState::Running;
        Ok(())
    }

    pub fn mark_finished(
        &mut self,
        id: PlanNodeId,
        outcome: NodeOutcome,
    ) -> Result<ExecutionTransition, TransitionError> {
        let state = self
            .node_states
            .get_mut(node_index(id))
            .ok_or(TransitionError::UnknownNode(id))?;
        if !matches!(state, StepState::Running) {
            return Err(TransitionError::NotRunning(id));
        }
        *state = match outcome {
            NodeOutcome::Succeeded => StepState::Succeeded,
            NodeOutcome::Failed(failure) => StepState::Failed { failure },
        };
        Ok(self.advance())
    }

    /// Cancel the complete execution and return process steps to terminate.
    pub fn cancel(&mut self, mode: CancelMode) -> ExecutionTransition {
        if self.state().is_terminal() {
            return ExecutionTransition::default();
        }

        let (execution_reason, step_reason) = match mode {
            CancelMode::Graceful => (ExecutionCancelReason::User, StepCancelReason::User),
            CancelMode::Force => (ExecutionCancelReason::Forced, StepCancelReason::Forced),
        };
        let nodes = self.nodes();
        let mut transition = ExecutionTransition::default();
        for node in nodes {
            let state = &mut self.node_states[node_index(node.id)];
            if matches!(state, StepState::Running)
                && let Some(step_id) = node.step_id
            {
                transition.to_cancel.push(step_id);
            }
            if !state.is_terminal() {
                *state = StepState::Cancelled {
                    reason: step_reason,
                };
            }
        }
        self.cancelled = Some(execution_reason);
        transition
    }
}

impl ExecutionPlan {
    pub fn pipeline(pipeline: Pipeline) -> Self {
        Self::Pipeline { pipeline }
    }

    pub fn validate(&self) -> Result<(), PlanValidationError> {
        validate_plan(self, "plan")
    }

    pub fn node_count(&self) -> usize {
        match self {
            Self::Pipeline { .. } | Self::ContextDelta { .. } => 1,
            Self::OnSuccess { left, right }
            | Self::OnFailure { left, right }
            | Self::Always { left, right } => left.node_count() + right.node_count(),
            Self::ParallelAll { branches } | Self::AnySuccess { branches } => {
                branches.iter().map(Self::node_count).sum()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanValidationError {
    pub path: String,
    pub message: String,
}

impl PlanValidationError {
    fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for PlanValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

impl Error for PlanValidationError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionError {
    UnknownNode(PlanNodeId),
    NotReady(PlanNodeId),
    NotRunning(PlanNodeId),
}

impl fmt::Display for TransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownNode(id) => write!(f, "unknown execution node {}", id.0),
            Self::NotReady(id) => write!(f, "execution node {} is not ready", id.0),
            Self::NotRunning(id) => write!(f, "execution node {} is not running", id.0),
        }
    }
}

impl Error for TransitionError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubtreeState {
    Waiting,
    Succeeded,
    Failed,
    Cancelled,
}

fn validate_plan(plan: &ExecutionPlan, path: &str) -> Result<(), PlanValidationError> {
    match plan {
        ExecutionPlan::Pipeline { pipeline } => validate_pipeline(pipeline, path),
        ExecutionPlan::ContextDelta { delta } => {
            if delta.set.is_empty() && delta.unset.is_empty() && delta.cwd.is_none() {
                return Err(PlanValidationError::new(path, "context delta is empty"));
            }
            for key in delta.set.keys().chain(delta.unset.iter()) {
                if !is_valid_env_name(key) {
                    return Err(PlanValidationError::new(
                        path,
                        format!("invalid environment variable name {key:?}"),
                    ));
                }
            }
            if delta.set.keys().any(|key| delta.unset.contains(key)) {
                return Err(PlanValidationError::new(
                    path,
                    "context delta cannot set and unset the same variable",
                ));
            }
            Ok(())
        }
        ExecutionPlan::OnSuccess { left, right }
        | ExecutionPlan::OnFailure { left, right }
        | ExecutionPlan::Always { left, right } => {
            validate_plan(left, &format!("{path}.left"))?;
            validate_plan(right, &format!("{path}.right"))
        }
        ExecutionPlan::ParallelAll { branches } | ExecutionPlan::AnySuccess { branches } => {
            if branches.len() < 2 {
                return Err(PlanValidationError::new(
                    path,
                    "parallel composition requires at least two branches",
                ));
            }
            for (index, branch) in branches.iter().enumerate() {
                validate_plan(branch, &format!("{path}.branches[{index}]"))?;
            }
            Ok(())
        }
    }
}

fn validate_pipeline(pipeline: &Pipeline, path: &str) -> Result<(), PlanValidationError> {
    if pipeline.segments.is_empty() {
        return Err(PlanValidationError::new(path, "pipeline has no segments"));
    }
    let last = pipeline.segments.len() - 1;
    for (index, segment) in pipeline.segments.iter().enumerate() {
        let segment_path = format!("{path}.segments[{index}]");
        if segment.command.first().is_none_or(String::is_empty) {
            return Err(PlanValidationError::new(
                segment_path,
                "pipeline segment has no command",
            ));
        }
        for key in segment.env.keys() {
            if !is_valid_env_name(key) {
                return Err(PlanValidationError::new(
                    &segment_path,
                    format!("invalid environment variable name {key:?}"),
                ));
            }
        }
        if index == last && segment.pipe_to_next.is_some() {
            return Err(PlanValidationError::new(
                segment_path,
                "last pipeline segment cannot pipe to a successor",
            ));
        }
        if index < last && segment.pipe_to_next.is_none() {
            return Err(PlanValidationError::new(
                segment_path,
                "non-final pipeline segment must declare its pipe operator",
            ));
        }
    }
    Ok(())
}

fn is_valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn collect_nodes(
    plan: &ExecutionPlan,
    execution_id: ExecutionId,
    next_step: &mut u32,
    nodes: &mut Vec<ExecutionNode>,
) {
    match plan {
        ExecutionPlan::Pipeline { pipeline } => {
            let step_id = StepId {
                execution: execution_id,
                index: *next_step,
            };
            *next_step += 1;
            nodes.push(ExecutionNode {
                id: PlanNodeId((nodes.len() + 1) as u32),
                step_id: Some(step_id),
                action: ExecutionAction::Pipeline(pipeline.clone()),
            });
        }
        ExecutionPlan::ContextDelta { delta } => nodes.push(ExecutionNode {
            id: PlanNodeId((nodes.len() + 1) as u32),
            step_id: None,
            action: ExecutionAction::ContextDelta(delta.clone()),
        }),
        ExecutionPlan::OnSuccess { left, right }
        | ExecutionPlan::OnFailure { left, right }
        | ExecutionPlan::Always { left, right } => {
            collect_nodes(left, execution_id, next_step, nodes);
            collect_nodes(right, execution_id, next_step, nodes);
        }
        ExecutionPlan::ParallelAll { branches } | ExecutionPlan::AnySuccess { branches } => {
            for branch in branches {
                collect_nodes(branch, execution_id, next_step, nodes);
            }
        }
    }
}

fn drive(
    plan: &ExecutionPlan,
    offset: usize,
    step_ids: &[Option<StepId>],
    states: &mut [StepState],
    transition: &mut ExecutionTransition,
) -> (usize, SubtreeState) {
    match plan {
        ExecutionPlan::Pipeline { .. } | ExecutionPlan::ContextDelta { .. } => {
            let id = PlanNodeId((offset + 1) as u32);
            let state = &states[offset];
            if matches!(state, StepState::Queued) {
                transition.newly_ready.push(id);
            }
            (offset + 1, leaf_subtree_state(state))
        }
        ExecutionPlan::OnSuccess { left, right } => {
            let (right_offset, left_state) = drive(left, offset, step_ids, states, transition);
            match left_state {
                SubtreeState::Waiting => (right_offset + right.node_count(), SubtreeState::Waiting),
                SubtreeState::Succeeded => drive(right, right_offset, step_ids, states, transition),
                SubtreeState::Failed => {
                    cancel_subtree(
                        right,
                        right_offset,
                        step_ids,
                        StepCancelReason::ConditionNotMet,
                        states,
                        transition,
                    );
                    (right_offset + right.node_count(), SubtreeState::Failed)
                }
                SubtreeState::Cancelled => {
                    cancel_subtree(
                        right,
                        right_offset,
                        step_ids,
                        StepCancelReason::ConditionNotMet,
                        states,
                        transition,
                    );
                    (right_offset + right.node_count(), SubtreeState::Cancelled)
                }
            }
        }
        ExecutionPlan::OnFailure { left, right } => {
            let (right_offset, left_state) = drive(left, offset, step_ids, states, transition);
            match left_state {
                SubtreeState::Waiting => (right_offset + right.node_count(), SubtreeState::Waiting),
                SubtreeState::Failed => drive(right, right_offset, step_ids, states, transition),
                SubtreeState::Succeeded => {
                    cancel_subtree(
                        right,
                        right_offset,
                        step_ids,
                        StepCancelReason::ConditionNotMet,
                        states,
                        transition,
                    );
                    (right_offset + right.node_count(), SubtreeState::Succeeded)
                }
                SubtreeState::Cancelled => {
                    cancel_subtree(
                        right,
                        right_offset,
                        step_ids,
                        StepCancelReason::ConditionNotMet,
                        states,
                        transition,
                    );
                    (right_offset + right.node_count(), SubtreeState::Cancelled)
                }
            }
        }
        ExecutionPlan::Always { left, right } => {
            let (right_offset, left_state) = drive(left, offset, step_ids, states, transition);
            if left_state == SubtreeState::Waiting {
                return (right_offset + right.node_count(), SubtreeState::Waiting);
            }
            let (end, right_state) = drive(right, right_offset, step_ids, states, transition);
            let state = if right_state == SubtreeState::Waiting {
                SubtreeState::Waiting
            } else if left_state == SubtreeState::Failed || right_state == SubtreeState::Failed {
                SubtreeState::Failed
            } else if left_state == SubtreeState::Cancelled
                || right_state == SubtreeState::Cancelled
            {
                SubtreeState::Cancelled
            } else {
                SubtreeState::Succeeded
            };
            (end, state)
        }
        ExecutionPlan::ParallelAll { branches } => {
            let mut next = offset;
            let mut branch_states = Vec::with_capacity(branches.len());
            for branch in branches {
                let (end, state) = drive(branch, next, step_ids, states, transition);
                next = end;
                branch_states.push(state);
            }
            (next, aggregate_parallel_all(&branch_states))
        }
        ExecutionPlan::AnySuccess { branches } => {
            let mut next = offset;
            let mut ranges = Vec::with_capacity(branches.len());
            let mut branch_states = Vec::with_capacity(branches.len());
            for branch in branches {
                let branch_offset = next;
                let (end, state) = drive(branch, next, step_ids, states, transition);
                next = end;
                ranges.push((branch_offset, branch));
                branch_states.push(state);
            }

            if branch_states.contains(&SubtreeState::Succeeded) {
                for ((branch_offset, branch), state) in ranges.into_iter().zip(&branch_states) {
                    if *state != SubtreeState::Succeeded {
                        cancel_subtree(
                            branch,
                            branch_offset,
                            step_ids,
                            StepCancelReason::AnySuccessSatisfied,
                            states,
                            transition,
                        );
                    }
                }
                (next, SubtreeState::Succeeded)
            } else if branch_states.contains(&SubtreeState::Waiting) {
                (next, SubtreeState::Waiting)
            } else if branch_states.contains(&SubtreeState::Failed) {
                (next, SubtreeState::Failed)
            } else {
                (next, SubtreeState::Cancelled)
            }
        }
    }
}

fn aggregate_parallel_all(states: &[SubtreeState]) -> SubtreeState {
    if states.contains(&SubtreeState::Waiting) {
        SubtreeState::Waiting
    } else if states.contains(&SubtreeState::Failed) {
        SubtreeState::Failed
    } else if states.contains(&SubtreeState::Cancelled) {
        SubtreeState::Cancelled
    } else {
        SubtreeState::Succeeded
    }
}

fn cancel_subtree(
    plan: &ExecutionPlan,
    offset: usize,
    step_ids: &[Option<StepId>],
    reason: StepCancelReason,
    states: &mut [StepState],
    transition: &mut ExecutionTransition,
) {
    for index in offset..offset + plan.node_count() {
        let state = &mut states[index];
        if matches!(state, StepState::Running)
            && let Some(step_id) = step_ids[index]
        {
            transition.to_cancel.push(step_id);
        }
        if !state.is_terminal() {
            *state = StepState::Cancelled { reason };
        }
    }
}

fn leaf_subtree_state(state: &StepState) -> SubtreeState {
    match state {
        StepState::Queued | StepState::Running => SubtreeState::Waiting,
        StepState::Succeeded => SubtreeState::Succeeded,
        StepState::Failed { .. } => SubtreeState::Failed,
        StepState::Cancelled { .. } => SubtreeState::Cancelled,
    }
}

const fn node_index(id: PlanNodeId) -> usize {
    match id.0.checked_sub(1) {
        Some(index) => index as usize,
        None => usize::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{PipeOp, PipeSegment};
    use std::collections::BTreeMap;

    fn pipeline(command: &str) -> ExecutionPlan {
        ExecutionPlan::pipeline(Pipeline::simple(vec![command.into()]))
    }

    fn spec(plan: ExecutionPlan) -> ExecutionSpec {
        ExecutionSpec {
            plan,
            start_scope: None,
            launch_context: LaunchContext::default(),
            source: None,
            retry_of: None,
        }
    }

    fn execution(plan: ExecutionPlan) -> Execution {
        Execution::new(ExecutionId(7), spec(plan)).expect("valid execution")
    }

    fn succeed(execution: &mut Execution, node: u32) -> ExecutionTransition {
        let id = PlanNodeId(node);
        execution.mark_running(id).expect("node is ready");
        execution
            .mark_finished(id, NodeOutcome::Succeeded)
            .expect("node is running")
    }

    fn fail(execution: &mut Execution, node: u32) -> ExecutionTransition {
        let id = PlanNodeId(node);
        execution.mark_running(id).expect("node is ready");
        execution
            .mark_finished(id, NodeOutcome::Failed(StepFailure::Exit { code: 2 }))
            .expect("node is running")
    }

    #[test]
    fn process_steps_have_stable_ids_and_context_deltas_do_not() {
        let plan = ExecutionPlan::OnSuccess {
            left: Box::new(ExecutionPlan::ContextDelta {
                delta: EnvDelta {
                    set: BTreeMap::from([("FOO".into(), "bar".into())]),
                    unset: Vec::new(),
                    cwd: None,
                },
            }),
            right: Box::new(ExecutionPlan::ParallelAll {
                branches: vec![pipeline("a"), pipeline("b")],
            }),
        };
        let execution = execution(plan);

        assert_eq!(
            execution
                .nodes()
                .iter()
                .map(|node| node.step_id)
                .collect::<Vec<_>>(),
            vec![
                None,
                Some(StepId {
                    execution: ExecutionId(7),
                    index: 1,
                }),
                Some(StepId {
                    execution: ExecutionId(7),
                    index: 2,
                }),
            ]
        );
    }

    #[test]
    fn on_success_runs_right_only_after_success() {
        let mut execution = execution(ExecutionPlan::OnSuccess {
            left: Box::new(pipeline("a")),
            right: Box::new(pipeline("b")),
        });

        assert_eq!(execution.advance().newly_ready, vec![PlanNodeId(1)]);
        assert_eq!(succeed(&mut execution, 1).newly_ready, vec![PlanNodeId(2)]);
        succeed(&mut execution, 2);
        assert_eq!(execution.state(), ExecutionState::Succeeded);
    }

    #[test]
    fn on_success_skips_right_after_failure() {
        let mut execution = execution(ExecutionPlan::OnSuccess {
            left: Box::new(pipeline("a")),
            right: Box::new(pipeline("b")),
        });

        assert!(fail(&mut execution, 1).newly_ready.is_empty());
        assert_eq!(execution.state(), ExecutionState::Failed);
        assert_eq!(
            execution.node_state(PlanNodeId(2)),
            Some(&StepState::Cancelled {
                reason: StepCancelReason::ConditionNotMet,
            })
        );
    }

    #[test]
    fn on_failure_recovers_with_successful_right_branch() {
        let mut execution = execution(ExecutionPlan::OnFailure {
            left: Box::new(pipeline("a")),
            right: Box::new(pipeline("recover")),
        });

        assert_eq!(fail(&mut execution, 1).newly_ready, vec![PlanNodeId(2)]);
        succeed(&mut execution, 2);
        assert_eq!(execution.state(), ExecutionState::Succeeded);
    }

    #[test]
    fn on_failure_skips_right_after_success() {
        let mut execution = execution(ExecutionPlan::OnFailure {
            left: Box::new(pipeline("a")),
            right: Box::new(pipeline("recover")),
        });

        succeed(&mut execution, 1);
        assert_eq!(execution.state(), ExecutionState::Succeeded);
        assert!(matches!(
            execution.node_state(PlanNodeId(2)),
            Some(StepState::Cancelled {
                reason: StepCancelReason::ConditionNotMet
            })
        ));
    }

    #[test]
    fn always_runs_right_and_preserves_failure() {
        let mut execution = execution(ExecutionPlan::Always {
            left: Box::new(pipeline("a")),
            right: Box::new(pipeline("cleanup")),
        });

        assert_eq!(fail(&mut execution, 1).newly_ready, vec![PlanNodeId(2)]);
        succeed(&mut execution, 2);
        assert_eq!(execution.state(), ExecutionState::Failed);
    }

    #[test]
    fn parallel_all_starts_every_branch_and_waits_for_all() {
        let mut execution = execution(ExecutionPlan::ParallelAll {
            branches: vec![pipeline("a"), pipeline("b")],
        });

        assert_eq!(
            execution.advance().newly_ready,
            vec![PlanNodeId(1), PlanNodeId(2)]
        );
        execution.mark_running(PlanNodeId(1)).unwrap();
        execution.mark_running(PlanNodeId(2)).unwrap();
        execution
            .mark_finished(PlanNodeId(1), NodeOutcome::Succeeded)
            .unwrap();
        assert_eq!(execution.state(), ExecutionState::Running);
        execution
            .mark_finished(PlanNodeId(2), NodeOutcome::Succeeded)
            .unwrap();
        assert_eq!(execution.state(), ExecutionState::Succeeded);
    }

    #[test]
    fn any_success_cancels_running_loser() {
        let mut execution = execution(ExecutionPlan::AnySuccess {
            branches: vec![pipeline("fast"), pipeline("slow")],
        });
        execution.mark_running(PlanNodeId(1)).unwrap();
        execution.mark_running(PlanNodeId(2)).unwrap();

        let transition = execution
            .mark_finished(PlanNodeId(1), NodeOutcome::Succeeded)
            .unwrap();

        assert_eq!(
            transition.to_cancel,
            vec![StepId {
                execution: ExecutionId(7),
                index: 2,
            }]
        );
        assert_eq!(execution.state(), ExecutionState::Succeeded);
        assert_eq!(
            execution.node_state(PlanNodeId(2)),
            Some(&StepState::Cancelled {
                reason: StepCancelReason::AnySuccessSatisfied,
            })
        );
    }

    #[test]
    fn nested_any_success_uses_global_process_step_ids() {
        let mut execution = execution(ExecutionPlan::OnSuccess {
            left: Box::new(ExecutionPlan::ContextDelta {
                delta: EnvDelta {
                    set: BTreeMap::from([("FOO".into(), "bar".into())]),
                    unset: Vec::new(),
                    cwd: None,
                },
            }),
            right: Box::new(ExecutionPlan::AnySuccess {
                branches: vec![pipeline("fast"), pipeline("slow")],
            }),
        });
        succeed(&mut execution, 1);
        execution.mark_running(PlanNodeId(2)).unwrap();
        execution.mark_running(PlanNodeId(3)).unwrap();

        let transition = execution
            .mark_finished(PlanNodeId(2), NodeOutcome::Succeeded)
            .unwrap();

        assert_eq!(
            transition.to_cancel,
            vec![StepId {
                execution: ExecutionId(7),
                index: 2,
            }]
        );
    }

    #[test]
    fn any_success_fails_only_after_every_branch_fails() {
        let mut execution = execution(ExecutionPlan::AnySuccess {
            branches: vec![pipeline("a"), pipeline("b")],
        });
        execution.mark_running(PlanNodeId(1)).unwrap();
        execution.mark_running(PlanNodeId(2)).unwrap();
        execution
            .mark_finished(
                PlanNodeId(1),
                NodeOutcome::Failed(StepFailure::Exit { code: 1 }),
            )
            .unwrap();
        assert_eq!(execution.state(), ExecutionState::Running);
        execution
            .mark_finished(
                PlanNodeId(2),
                NodeOutcome::Failed(StepFailure::Exit { code: 2 }),
            )
            .unwrap();
        assert_eq!(execution.state(), ExecutionState::Failed);
    }

    #[test]
    fn force_cancel_records_reason_and_returns_running_processes() {
        let mut execution = execution(ExecutionPlan::ParallelAll {
            branches: vec![pipeline("a"), pipeline("b")],
        });
        execution.mark_running(PlanNodeId(1)).unwrap();

        let transition = execution.cancel(CancelMode::Force);

        assert_eq!(
            transition.to_cancel,
            vec![StepId {
                execution: ExecutionId(7),
                index: 1,
            }]
        );
        assert_eq!(
            execution.state(),
            ExecutionState::Cancelled {
                reason: ExecutionCancelReason::Forced,
            }
        );
    }

    #[test]
    fn validation_rejects_malformed_pipeline_and_parallel_plan() {
        let malformed_pipeline = ExecutionPlan::pipeline(Pipeline {
            segments: vec![
                PipeSegment {
                    env: BTreeMap::new(),
                    command: vec!["a".into()],
                    pipe_to_next: None,
                },
                PipeSegment {
                    env: BTreeMap::new(),
                    command: vec!["b".into()],
                    pipe_to_next: None,
                },
            ],
        });
        assert!(malformed_pipeline.validate().is_err());

        let one_branch = ExecutionPlan::ParallelAll {
            branches: vec![pipeline("a")],
        };
        assert!(one_branch.validate().is_err());

        let valid_pipeline = ExecutionPlan::pipeline(Pipeline {
            segments: vec![
                PipeSegment {
                    env: BTreeMap::from([("FOO".into(), "bar".into())]),
                    command: vec!["a".into()],
                    pipe_to_next: Some(PipeOp::Stdout),
                },
                PipeSegment {
                    env: BTreeMap::new(),
                    command: vec!["b".into()],
                    pipe_to_next: None,
                },
            ],
        });
        valid_pipeline.validate().unwrap();
    }

    #[test]
    fn spawn_adapter_debug_redacts_token() {
        let handle = SpawnAdapterHandle {
            endpoint: PathBuf::from("/run/user/1/cue/adapters/a.sock"),
            token: "secret-token".into(),
        };

        let debug = format!("{handle:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret-token"));
    }

    #[test]
    fn strict_execution_spec_rejects_unknown_fields() {
        let json = r#"{
            "plan":{"kind":"pipeline","pipeline":{"segments":[{"command":["true"],"pipe_to_next":null}]}},
            "launch_context":{},
            "unknown":true
        }"#;

        assert!(serde_json::from_str::<ExecutionSpec>(json).is_err());

        let nested = r#"{
            "plan":{"kind":"pipeline","pipeline":{"segments":[{"command":["true"],"pipe_to_next":null}]},"unknown":true},
            "launch_context":{}
        }"#;
        assert!(serde_json::from_str::<ExecutionSpec>(nested).is_err());
    }
}
