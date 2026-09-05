//! Pure reducer for Cue execution plans.
//!
//! The reducer owns orchestration decisions and durable leaf state. Runtime
//! code realizes ready work and reports typed completions; it does not decide
//! which branch runs next, how scope flows through the plan, or when a cancel
//! request becomes a terminal fact.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ExecutionId, ScopeHash, StepId};

use super::{
    AbsolutePath, BuiltinCommand, ExecutionPlan, ExecutionSpec, IoMode, ParallelJoin, Pipeline,
    PlanValidationError, Scope, SequenceCondition,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum StepState {
    Pending,
    Running,
    Cancelling {
        cause: StepCancelCause,
        mode: CancelMode,
    },
    Succeeded,
    Failed {
        failure: StepFailure,
    },
    Skipped {
        reason: SkipReason,
    },
    Cancelled {
        cause: StepCancelCause,
    },
}

impl StepState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed { .. } | Self::Skipped { .. } | Self::Cancelled { .. }
        )
    }

    fn is_active(&self) -> bool {
        matches!(self, Self::Running | Self::Cancelling { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StepFailure {
    Exit { code: i32 },
    Signal { signal: i32 },
    Spawn { message: String },
    Builtin { message: String },
    Infrastructure { message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    ConditionNotMet,
    AnySuccessSatisfied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepCancelCause {
    ExecutionRequested,
    AnySuccessSatisfied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelMode {
    Graceful,
    Force,
}

impl CancelMode {
    const fn strength(self) -> u8 {
        match self {
            Self::Graceful => 0,
            Self::Force => 1,
        }
    }

    const fn stronger_than(self, other: Self) -> bool {
        self.strength() > other.strength()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionState {
    Pending,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
}

impl ExecutionState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

/// Durable state for one executable leaf. Scope payloads live in the external
/// content-addressed scope store; this record carries hashes only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepRecord {
    id: StepId,
    state: StepState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input_scope: Option<ScopeHash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    output_scope: Option<ScopeHash>,
}

impl StepRecord {
    pub const fn id(&self) -> StepId {
        self.id
    }

    pub fn state(&self) -> &StepState {
        &self.state
    }

    pub const fn input_scope(&self) -> Option<ScopeHash> {
        self.input_scope
    }

    pub const fn output_scope(&self) -> Option<ScopeHash> {
        self.output_scope
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepAction {
    Builtin(BuiltinCommand),
    Run { pipeline: Pipeline, io: IoMode },
}

/// Completion of a process Run. Cancellation is distinct from a signal or
/// other failure because only an accepted cancellation intent may terminate a
/// Step as `Cancelled`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunCompletion {
    Succeeded,
    Failed(StepFailure),
    Cancelled,
}

/// A successful builtin report. The runtime resolves only the filesystem
/// dependent result of `cd`; Env and Umask are applied by this reducer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuiltinSuccess {
    Cd { cwd: AbsolutePath },
    Env,
    Umask,
}

/// Work and content-addressed scopes produced by one state transition.
/// Persist these scopes and the snapshot together with runtime follow-up before
/// realizing any Step. The follow-up set contains no frozen semantic payload.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutionTransition {
    pub runtime_steps: BTreeSet<StepId>,
    pub new_scopes: Vec<Scope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSnapshot {
    pub id: ExecutionId,
    pub spec: ExecutionSpec,
    pub steps: Vec<StepRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_requested: Option<CancelMode>,
}

/// The sole mutable lifecycle state for one execution plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Execution {
    id: ExecutionId,
    spec: ExecutionSpec,
    steps: Vec<StepRecord>,
    cancel_requested: Option<CancelMode>,
}

impl Execution {
    pub fn new(id: ExecutionId, spec: ExecutionSpec) -> Self {
        let steps = (1..=plan_leaf_count(spec.plan()) as u32)
            .map(|index| StepRecord {
                id: StepId {
                    execution: id,
                    index,
                },
                state: StepState::Pending,
                input_scope: None,
                output_scope: None,
            })
            .collect();
        Self {
            id,
            spec,
            steps,
            cancel_requested: None,
        }
    }

    pub const fn id(&self) -> ExecutionId {
        self.id
    }

    pub fn spec(&self) -> &ExecutionSpec {
        &self.spec
    }

    pub fn steps(&self) -> &[StepRecord] {
        &self.steps
    }

    pub const fn cancel_requested(&self) -> Option<CancelMode> {
        self.cancel_requested
    }

    pub fn step(&self, id: StepId) -> Option<&StepRecord> {
        self.step_index(id).ok().map(|index| &self.steps[index])
    }

    pub fn action(&self, id: StepId) -> Option<StepAction> {
        let index = self.step_index(id).ok()?;
        action_at(self.spec.plan(), index)
    }

    pub fn snapshot(&self) -> ExecutionSnapshot {
        ExecutionSnapshot {
            id: self.id,
            spec: self.spec.clone(),
            steps: self.steps.clone(),
            cancel_requested: self.cancel_requested,
        }
    }

    pub fn restore(snapshot: ExecutionSnapshot) -> Result<Self, ExecutionError> {
        validate_snapshot(&snapshot)?;
        Ok(Self {
            id: snapshot.id,
            spec: snapshot.spec,
            steps: snapshot.steps,
            cancel_requested: snapshot.cancel_requested,
        })
    }

    pub fn state(&self) -> ExecutionState {
        let result = evaluate(self.spec.plan(), 0, self.spec.scope(), &self.steps).1;
        match result.status {
            SubtreeStatus::Succeeded => ExecutionState::Succeeded,
            SubtreeStatus::Failed => ExecutionState::Failed,
            SubtreeStatus::Cancelled => ExecutionState::Cancelled,
            SubtreeStatus::Waiting | SubtreeStatus::Skipped => {
                if self.cancel_requested.is_some()
                    && self.steps.iter().any(|step| step.state.is_active())
                {
                    return ExecutionState::Cancelling;
                }
                if self.steps.iter().any(|step| {
                    matches!(
                        step.state,
                        StepState::Running
                            | StepState::Cancelling { .. }
                            | StepState::Succeeded
                            | StepState::Failed { .. }
                            | StepState::Cancelled { .. }
                    )
                }) {
                    ExecutionState::Running
                } else {
                    ExecutionState::Pending
                }
            }
        }
    }

    /// Compute ready work and apply deterministic condition/race decisions.
    pub fn advance(&mut self) -> Result<ExecutionTransition, ExecutionError> {
        let mut steps = self.steps.clone();
        let mut transition = ExecutionTransition::default();
        drive(
            self.spec.plan(),
            0,
            self.spec.scope(),
            &mut steps,
            &mut transition,
        )?;
        self.steps = steps;
        Ok(transition)
    }

    /// Complete cancellation of either kind of leaf after realization is quiescent.
    pub fn complete_cancelled(
        &mut self,
        id: StepId,
    ) -> Result<ExecutionTransition, ExecutionError> {
        let index = self.active_step_index(id)?;
        let StepState::Cancelling { cause, .. } = self.steps[index].state else {
            return Err(ExecutionError::UnexpectedRunCancellation(id));
        };
        let previous = self.steps[index].clone();
        self.steps[index].state = StepState::Cancelled { cause };
        match self.advance() {
            Ok(transition) => Ok(transition),
            Err(error) => {
                self.steps[index] = previous;
                Err(error)
            }
        }
    }

    pub fn complete_run(
        &mut self,
        id: StepId,
        completion: RunCompletion,
    ) -> Result<ExecutionTransition, ExecutionError> {
        if !matches!(self.action(id), Some(StepAction::Run { .. })) {
            return Err(ExecutionError::WrongStepAction {
                step: id,
                expected: "run",
            });
        }
        let index = self.active_step_index(id)?;
        let input_scope = self.steps[index]
            .input_scope
            .ok_or(ExecutionError::MissingInputScope(id))?;
        let previous = self.steps[index].clone();
        let cancel_reason = match self.steps[index].state {
            StepState::Cancelling { cause, .. } => Some(cause),
            StepState::Running => None,
            _ => unreachable!("active_step_index checked state"),
        };
        match completion {
            RunCompletion::Succeeded => {
                self.steps[index].state = StepState::Succeeded;
                self.steps[index].output_scope = Some(input_scope);
            }
            RunCompletion::Failed(failure) => {
                validate_failure(&failure)?;
                self.steps[index].state = StepState::Failed { failure };
                self.steps[index].output_scope = Some(input_scope);
            }
            RunCompletion::Cancelled => {
                let reason = cancel_reason.ok_or(ExecutionError::UnexpectedRunCancellation(id))?;
                self.steps[index].state = StepState::Cancelled { cause: reason };
                self.steps[index].output_scope = None;
            }
        }
        match self.advance() {
            Ok(transition) => Ok(transition),
            Err(error) => {
                self.steps[index] = previous;
                Err(error)
            }
        }
    }

    pub fn complete_builtin(
        &mut self,
        id: StepId,
        input_scope: &Scope,
        result: Result<BuiltinSuccess, StepFailure>,
    ) -> Result<ExecutionTransition, ExecutionError> {
        let Some(StepAction::Builtin(command)) = self.action(id) else {
            return Err(ExecutionError::WrongStepAction {
                step: id,
                expected: "builtin",
            });
        };
        let index = self.active_step_index(id)?;
        let expected_scope = self.steps[index]
            .input_scope
            .ok_or(ExecutionError::MissingInputScope(id))?;
        let actual_scope = input_scope.compute_hash();
        if actual_scope != expected_scope {
            return Err(ExecutionError::InputScopeMismatch {
                step: id,
                expected: expected_scope,
                actual: actual_scope,
            });
        }

        let (state, output) = match result {
            Err(failure) => {
                validate_failure(&failure)?;
                (StepState::Failed { failure }, input_scope.clone())
            }
            Ok(success) => (
                StepState::Succeeded,
                apply_builtin(id, &command, input_scope, success)?,
            ),
        };
        let output_hash = output.compute_hash();
        let previous = self.steps[index].clone();
        self.steps[index].state = state;
        self.steps[index].output_scope = Some(output_hash);

        let mut transition = match self.advance() {
            Ok(transition) => transition,
            Err(error) => {
                self.steps[index] = previous;
                return Err(error);
            }
        };
        if output_hash != expected_scope {
            transition.new_scopes.push(output);
        }
        Ok(transition)
    }

    /// Mark all active work interrupted by a daemon restart as failed. Pending
    /// conditional branches remain eligible to advance from those failures.
    pub fn interrupt_running(&mut self, message: impl Into<String>) {
        let message = message.into();
        for (index, step) in self.steps.iter_mut().enumerate() {
            if step.state.is_active()
                && matches!(
                    action_at(self.spec.plan(), index),
                    Some(StepAction::Run { .. })
                )
            {
                step.state = StepState::Failed {
                    failure: StepFailure::Infrastructure {
                        message: message.clone(),
                    },
                };
                step.output_scope = step.input_scope;
            }
        }
    }

    pub fn cancel(&mut self, mode: CancelMode) -> ExecutionTransition {
        if self.state().is_terminal() {
            return ExecutionTransition::default();
        }

        if self
            .cancel_requested
            .is_some_and(|existing| !mode.stronger_than(existing))
        {
            return ExecutionTransition::default();
        }
        self.cancel_requested = Some(mode);
        let mut transition = ExecutionTransition::default();
        for step in &mut self.steps {
            request_step_cancel(
                step,
                StepCancelCause::ExecutionRequested,
                mode,
                &mut transition,
            );
        }
        transition
    }

    fn step_index(&self, id: StepId) -> Result<usize, ExecutionError> {
        if id.execution != self.id || id.index == 0 {
            return Err(ExecutionError::UnknownStep(id));
        }
        let index = (id.index - 1) as usize;
        if index >= self.steps.len() {
            return Err(ExecutionError::UnknownStep(id));
        }
        Ok(index)
    }

    fn active_step_index(&self, id: StepId) -> Result<usize, ExecutionError> {
        let index = self.step_index(id)?;
        if !self.steps[index].state.is_active() {
            return Err(ExecutionError::StepNotActive(id));
        }
        Ok(index)
    }
}

fn request_step_cancel(
    step: &mut StepRecord,
    cause: StepCancelCause,
    mode: CancelMode,
    transition: &mut ExecutionTransition,
) {
    match step.state {
        StepState::Pending => {
            step.state = StepState::Cancelled { cause };
        }
        StepState::Running => {
            step.state = StepState::Cancelling { cause, mode };
            transition.runtime_steps.insert(step.id);
        }
        StepState::Cancelling {
            cause: first_cause,
            mode: existing,
        } if mode.stronger_than(existing) => {
            step.state = StepState::Cancelling {
                cause: first_cause,
                mode,
            };
            transition.runtime_steps.insert(step.id);
        }
        _ => {}
    }
}

fn validate_failure(failure: &StepFailure) -> Result<(), ExecutionError> {
    if matches!(failure, StepFailure::Exit { code: 0 }) {
        return Err(ExecutionError::ZeroExitFailure);
    }
    Ok(())
}

fn apply_builtin(
    step: StepId,
    command: &BuiltinCommand,
    input: &Scope,
    success: BuiltinSuccess,
) -> Result<Scope, ExecutionError> {
    match (command, success) {
        (BuiltinCommand::Cd(_), BuiltinSuccess::Cd { cwd }) => Ok(input.with_cwd(cwd)),
        (BuiltinCommand::Env(mutation), BuiltinSuccess::Env) => {
            Ok(input.apply_env(mutation.patch()))
        }
        (BuiltinCommand::Umask(mask), BuiltinSuccess::Umask) => Ok(input.with_umask(*mask)),
        (command, success) => Err(ExecutionError::WrongBuiltinSuccess {
            step,
            command: builtin_name(command),
            success: builtin_success_name(&success),
        }),
    }
}

fn builtin_name(command: &BuiltinCommand) -> &'static str {
    match command {
        BuiltinCommand::Cd(_) => "cd",
        BuiltinCommand::Env(_) => "env",
        BuiltinCommand::Umask(_) => "umask",
    }
}

fn builtin_success_name(success: &BuiltinSuccess) -> &'static str {
    match success {
        BuiltinSuccess::Cd { .. } => "cd",
        BuiltinSuccess::Env => "env",
        BuiltinSuccess::Umask => "umask",
    }
}

fn validate_snapshot(snapshot: &ExecutionSnapshot) -> Result<(), ExecutionError> {
    let expected = snapshot.spec.plan().leaf_count()? as usize;
    if snapshot.steps.len() != expected {
        return Err(ExecutionError::SnapshotStepCount {
            expected,
            actual: snapshot.steps.len(),
        });
    }
    for (offset, step) in snapshot.steps.iter().enumerate() {
        let expected_id = StepId {
            execution: snapshot.id,
            index: (offset + 1) as u32,
        };
        if step.id != expected_id {
            return Err(ExecutionError::SnapshotStepId {
                expected: expected_id,
                actual: step.id,
            });
        }
        if matches!(step.state, StepState::Pending | StepState::Skipped { .. })
            && step.input_scope.is_some()
        {
            return Err(ExecutionError::InvalidSnapshot(step.id));
        }
        if let StepState::Failed { ref failure } = step.state {
            validate_failure(failure)?;
        }
        match step.state {
            StepState::Running | StepState::Cancelling { .. } if step.input_scope.is_none() => {
                return Err(ExecutionError::MissingInputScope(step.id));
            }
            StepState::Succeeded | StepState::Failed { .. }
                if step.input_scope.is_none() || step.output_scope.is_none() =>
            {
                return Err(ExecutionError::MissingTerminalScope(step.id));
            }
            StepState::Pending
            | StepState::Running
            | StepState::Cancelling { .. }
            | StepState::Skipped { .. }
            | StepState::Cancelled { .. }
                if step.output_scope.is_some() =>
            {
                return Err(ExecutionError::UnexpectedOutputScope(step.id));
            }
            _ => {}
        }
    }
    if snapshot.cancel_requested.is_some()
        && snapshot
            .steps
            .iter()
            .any(|step| matches!(step.state, StepState::Pending | StepState::Running))
    {
        return Err(ExecutionError::InvalidSnapshot(snapshot.steps[0].id));
    }
    validate_scope_tree(
        snapshot.spec.plan(),
        0,
        snapshot.spec.scope(),
        &snapshot.steps,
        false,
        snapshot.cancel_requested,
    )?;
    Ok(())
}

fn validate_scope_tree(
    plan: &ExecutionPlan,
    offset: usize,
    input: ScopeHash,
    steps: &[StepRecord],
    any_success: bool,
    cancel_requested: Option<CancelMode>,
) -> Result<usize, ExecutionError> {
    match plan {
        ExecutionPlan::Builtin { .. } | ExecutionPlan::Run { .. } => {
            let step = &steps[offset];
            if step.input_scope.is_some_and(|scope| scope != input) {
                return Err(ExecutionError::InvalidSnapshot(step.id));
            }
            if step.output_scope.is_some()
                && (matches!(plan, ExecutionPlan::Run { .. })
                    || matches!(step.state, StepState::Failed { .. }))
                && step.output_scope != step.input_scope
            {
                return Err(ExecutionError::InvalidSnapshot(step.id));
            }
            match step.state {
                StepState::Cancelling { cause, mode } => {
                    if (cause == StepCancelCause::AnySuccessSatisfied
                        && (!any_success || mode != CancelMode::Force))
                        || (cause == StepCancelCause::ExecutionRequested
                            && cancel_requested.is_none())
                        || cancel_requested.is_some_and(|requested| requested.stronger_than(mode))
                    {
                        return Err(ExecutionError::InvalidSnapshot(step.id));
                    }
                }
                StepState::Cancelled { cause } => {
                    if (cause == StepCancelCause::AnySuccessSatisfied
                        && (!any_success || step.input_scope.is_none()))
                        || (cause == StepCancelCause::ExecutionRequested
                            && cancel_requested.is_none())
                    {
                        return Err(ExecutionError::InvalidSnapshot(step.id));
                    }
                }
                StepState::Skipped {
                    reason: SkipReason::AnySuccessSatisfied,
                } if !any_success => return Err(ExecutionError::InvalidSnapshot(step.id)),
                _ => {}
            }
            Ok(offset + 1)
        }
        ExecutionPlan::Sequence { first, then, when } => {
            let next =
                validate_scope_tree(first, offset, input, steps, any_success, cancel_requested)?;
            let (_, result) = evaluate(first, offset, input, steps);
            if !sequence_runs_then(*when, result.status) {
                for step in &steps[next..next + plan_leaf_count(then)] {
                    if step.input_scope.is_some() {
                        return Err(ExecutionError::InvalidSnapshot(step.id));
                    }
                }
            }
            validate_scope_tree(
                then,
                next,
                result.scope.unwrap_or(input),
                steps,
                any_success,
                cancel_requested,
            )
        }
        ExecutionPlan::Parallel { branches, join } => {
            let mut next = offset;
            let mut winner = false;
            for branch in branches.iter() {
                let (end, result) = evaluate(branch, next, input, steps);
                winner |= result.status == SubtreeStatus::Succeeded;
                next = end;
            }
            let has_winner = any_success || (*join == ParallelJoin::AnySuccess && winner);
            next = offset;
            for branch in branches.iter() {
                next =
                    validate_scope_tree(branch, next, input, steps, has_winner, cancel_requested)?;
            }
            Ok(next)
        }
    }
}

fn action_at(plan: &ExecutionPlan, target: usize) -> Option<StepAction> {
    let mut stack = vec![plan];
    let mut index = 0usize;
    while let Some(plan) = stack.pop() {
        match plan {
            ExecutionPlan::Builtin { command } => {
                if index == target {
                    return Some(StepAction::Builtin(command.clone()));
                }
                index += 1;
            }
            ExecutionPlan::Run { pipeline, io } => {
                if index == target {
                    return Some(StepAction::Run {
                        pipeline: pipeline.clone(),
                        io: *io,
                    });
                }
                index += 1;
            }
            ExecutionPlan::Sequence { first, then, .. } => {
                stack.push(then);
                stack.push(first);
            }
            ExecutionPlan::Parallel { branches, .. } => {
                for branch in branches.iter().rev() {
                    stack.push(branch);
                }
            }
        }
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubtreeStatus {
    Waiting,
    Succeeded,
    Failed,
    Skipped,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SubtreeResult {
    status: SubtreeStatus,
    scope: Option<ScopeHash>,
}

impl SubtreeResult {
    const fn waiting() -> Self {
        Self {
            status: SubtreeStatus::Waiting,
            scope: None,
        }
    }

    const fn terminal(status: SubtreeStatus, scope: Option<ScopeHash>) -> Self {
        Self { status, scope }
    }
}

fn drive(
    plan: &ExecutionPlan,
    offset: usize,
    input_scope: ScopeHash,
    steps: &mut [StepRecord],
    transition: &mut ExecutionTransition,
) -> Result<(usize, SubtreeResult), ExecutionError> {
    if steps[offset..offset + plan_leaf_count(plan)]
        .iter()
        .all(|step| matches!(step.state, StepState::Skipped { .. }))
    {
        return Ok((
            offset + plan_leaf_count(plan),
            SubtreeResult::terminal(SubtreeStatus::Skipped, None),
        ));
    }
    match plan {
        ExecutionPlan::Builtin { .. } | ExecutionPlan::Run { .. } => {
            drive_leaf(offset, input_scope, steps, transition)
        }
        ExecutionPlan::Sequence { first, then, when } => {
            let (then_offset, first_result) = drive(first, offset, input_scope, steps, transition)?;
            if first_result.status == SubtreeStatus::Waiting {
                return Ok((
                    then_offset + plan_leaf_count(then),
                    SubtreeResult::waiting(),
                ));
            }

            if sequence_runs_then(*when, first_result.status) {
                let then_input = first_result.scope.unwrap_or(input_scope);
                let (end, then_result) = drive(then, then_offset, then_input, steps, transition)?;
                Ok((end, sequence_result(*when, first_result, then_result)))
            } else {
                skip_condition_subtree(then, then_offset, steps)?;
                Ok((then_offset + plan_leaf_count(then), first_result))
            }
        }
        ExecutionPlan::Parallel { branches, join } => {
            if *join == ParallelJoin::AnySuccess {
                let mut next = offset;
                let mut branches_with_results = Vec::new();
                for branch in branches.iter() {
                    let (end, result) = evaluate(branch, next, input_scope, steps);
                    branches_with_results.push((next, branch, result));
                    next = end;
                }
                if branches_with_results
                    .iter()
                    .any(|(_, _, result)| result.status == SubtreeStatus::Succeeded)
                {
                    for (start, branch, result) in branches_with_results {
                        if result.status != SubtreeStatus::Succeeded {
                            cancel_any_success_loser(branch, start, steps, transition);
                        }
                    }
                    return Ok(evaluate(plan, offset, input_scope, steps));
                }
            }
            let mut next = offset;
            for branch in branches.iter() {
                next = drive(branch, next, input_scope, steps, transition)?.0;
            }
            Ok(evaluate(plan, offset, input_scope, steps))
        }
    }
}

fn drive_leaf(
    offset: usize,
    input_scope: ScopeHash,
    steps: &mut [StepRecord],
    transition: &mut ExecutionTransition,
) -> Result<(usize, SubtreeResult), ExecutionError> {
    let step = &mut steps[offset];
    if let Some(existing) = step.input_scope {
        if existing != input_scope {
            return Err(ExecutionError::InputScopeMismatch {
                step: step.id,
                expected: existing,
                actual: input_scope,
            });
        }
    } else if matches!(step.state, StepState::Pending) {
        step.input_scope = Some(input_scope);
    }

    if matches!(step.state, StepState::Pending) {
        step.state = StepState::Running;
        transition.runtime_steps.insert(step.id);
    }
    Ok((offset + 1, leaf_result(step, input_scope)))
}

fn leaf_result(step: &StepRecord, input_scope: ScopeHash) -> SubtreeResult {
    match step.state {
        StepState::Pending | StepState::Running | StepState::Cancelling { .. } => {
            SubtreeResult::waiting()
        }
        StepState::Succeeded => SubtreeResult::terminal(
            SubtreeStatus::Succeeded,
            step.output_scope.or(Some(input_scope)),
        ),
        StepState::Failed { .. } => SubtreeResult::terminal(
            SubtreeStatus::Failed,
            step.output_scope.or(Some(input_scope)),
        ),
        StepState::Skipped { .. } => SubtreeResult::terminal(SubtreeStatus::Skipped, None),
        StepState::Cancelled { .. } => SubtreeResult::terminal(
            SubtreeStatus::Cancelled,
            step.input_scope.or(Some(input_scope)),
        ),
    }
}

fn sequence_runs_then(condition: SequenceCondition, status: SubtreeStatus) -> bool {
    match condition {
        SequenceCondition::Success => status == SubtreeStatus::Succeeded,
        SequenceCondition::Failure => status == SubtreeStatus::Failed,
        SequenceCondition::Always => {
            !matches!(status, SubtreeStatus::Waiting | SubtreeStatus::Skipped)
        }
    }
}

fn sequence_result(
    condition: SequenceCondition,
    first: SubtreeResult,
    then: SubtreeResult,
) -> SubtreeResult {
    if then.status == SubtreeStatus::Waiting {
        return then;
    }
    if then.status == SubtreeStatus::Skipped {
        return first;
    }
    if condition != SequenceCondition::Always {
        return SubtreeResult::terminal(then.status, then.scope.or(first.scope));
    }

    let status = if first.status == SubtreeStatus::Failed || then.status == SubtreeStatus::Failed {
        SubtreeStatus::Failed
    } else if first.status == SubtreeStatus::Cancelled || then.status == SubtreeStatus::Cancelled {
        SubtreeStatus::Cancelled
    } else if first.status == SubtreeStatus::Skipped && then.status == SubtreeStatus::Skipped {
        SubtreeStatus::Skipped
    } else {
        SubtreeStatus::Succeeded
    };
    SubtreeResult::terminal(status, then.scope.or(first.scope))
}

fn parallel_all_result(results: &[SubtreeResult], input_scope: ScopeHash) -> SubtreeResult {
    let status = if results
        .iter()
        .any(|result| result.status == SubtreeStatus::Waiting)
    {
        SubtreeStatus::Waiting
    } else if results
        .iter()
        .any(|result| result.status == SubtreeStatus::Failed)
    {
        SubtreeStatus::Failed
    } else if results
        .iter()
        .any(|result| result.status == SubtreeStatus::Cancelled)
    {
        SubtreeStatus::Cancelled
    } else {
        SubtreeStatus::Succeeded
    };
    if status == SubtreeStatus::Waiting {
        SubtreeResult::waiting()
    } else {
        SubtreeResult::terminal(status, Some(input_scope))
    }
}

fn skip_condition_subtree(
    plan: &ExecutionPlan,
    offset: usize,
    steps: &mut [StepRecord],
) -> Result<(), ExecutionError> {
    for step in &mut steps[offset..offset + plan_leaf_count(plan)] {
        match step.state {
            StepState::Pending => {
                step.state = StepState::Skipped {
                    reason: SkipReason::ConditionNotMet,
                };
                step.output_scope = None;
            }
            StepState::Running | StepState::Cancelling { .. } => {
                return Err(ExecutionError::UnexpectedActiveConditionStep(step.id));
            }
            _ => {}
        }
    }
    Ok(())
}

fn cancel_any_success_loser(
    plan: &ExecutionPlan,
    offset: usize,
    steps: &mut [StepRecord],
    transition: &mut ExecutionTransition,
) {
    let count = plan_leaf_count(plan);
    for relative in 0..count {
        let step = &mut steps[offset + relative];
        match step.state {
            StepState::Pending => {
                step.state = StepState::Skipped {
                    reason: SkipReason::AnySuccessSatisfied,
                };
                step.output_scope = None;
            }
            StepState::Running | StepState::Cancelling { .. } => {
                request_step_cancel(
                    step,
                    StepCancelCause::AnySuccessSatisfied,
                    CancelMode::Force,
                    transition,
                );
            }
            _ => {}
        }
    }
}

fn evaluate(
    plan: &ExecutionPlan,
    offset: usize,
    input_scope: ScopeHash,
    steps: &[StepRecord],
) -> (usize, SubtreeResult) {
    if steps[offset..offset + plan_leaf_count(plan)]
        .iter()
        .all(|step| matches!(step.state, StepState::Skipped { .. }))
    {
        return (
            offset + plan_leaf_count(plan),
            SubtreeResult::terminal(SubtreeStatus::Skipped, None),
        );
    }
    match plan {
        ExecutionPlan::Builtin { .. } | ExecutionPlan::Run { .. } => {
            (offset + 1, leaf_result(&steps[offset], input_scope))
        }
        ExecutionPlan::Sequence { first, then, when } => {
            let (then_offset, first_result) = evaluate(first, offset, input_scope, steps);
            if first_result.status == SubtreeStatus::Waiting {
                return (
                    then_offset + plan_leaf_count(then),
                    SubtreeResult::waiting(),
                );
            }
            if sequence_runs_then(*when, first_result.status) {
                let then_input = first_result.scope.unwrap_or(input_scope);
                let (end, then_result) = evaluate(then, then_offset, then_input, steps);
                (end, sequence_result(*when, first_result, then_result))
            } else {
                (then_offset + plan_leaf_count(then), first_result)
            }
        }
        ExecutionPlan::Parallel { branches, join } => {
            let mut next = offset;
            let mut results = Vec::with_capacity(branches.as_slice().len());
            for branch in branches.iter() {
                let (end, result) = evaluate(branch, next, input_scope, steps);
                next = end;
                results.push(result);
            }
            let result = match join {
                ParallelJoin::All => parallel_all_result(&results, input_scope),
                ParallelJoin::AnySuccess => {
                    let has_success = results
                        .iter()
                        .any(|result| result.status == SubtreeStatus::Succeeded);
                    let has_waiting = results
                        .iter()
                        .any(|result| result.status == SubtreeStatus::Waiting);
                    if has_success && !has_waiting {
                        SubtreeResult::terminal(SubtreeStatus::Succeeded, Some(input_scope))
                    } else if has_waiting {
                        SubtreeResult::waiting()
                    } else if results
                        .iter()
                        .any(|result| result.status == SubtreeStatus::Failed)
                    {
                        SubtreeResult::terminal(SubtreeStatus::Failed, Some(input_scope))
                    } else {
                        SubtreeResult::terminal(SubtreeStatus::Cancelled, Some(input_scope))
                    }
                }
            };
            (next, result)
        }
    }
}

fn plan_leaf_count(plan: &ExecutionPlan) -> usize {
    match plan {
        ExecutionPlan::Builtin { .. } | ExecutionPlan::Run { .. } => 1,
        ExecutionPlan::Sequence { first, then, .. } => {
            plan_leaf_count(first) + plan_leaf_count(then)
        }
        ExecutionPlan::Parallel { branches, .. } => branches.iter().map(plan_leaf_count).sum(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ExecutionError {
    #[error(transparent)]
    InvalidPlan(#[from] PlanValidationError),
    #[error("unknown execution step {0}")]
    UnknownStep(StepId),
    #[error("zero exit code is success, not failure")]
    ZeroExitFailure,
    #[error("snapshot violates the plan or scope relationship at {0}")]
    InvalidSnapshot(StepId),
    #[error("step {0} is not active")]
    StepNotActive(StepId),
    #[error("run step {0} reported cancellation without a committed cancellation intent")]
    UnexpectedRunCancellation(StepId),
    #[error("condition-selected-out step {0} was already active")]
    UnexpectedActiveConditionStep(StepId),
    #[error("step {step} is not a {expected} leaf")]
    WrongStepAction {
        step: StepId,
        expected: &'static str,
    },
    #[error("step {step} expects {command} success, got {success} success")]
    WrongBuiltinSuccess {
        step: StepId,
        command: &'static str,
        success: &'static str,
    },
    #[error("step {0} has no input scope")]
    MissingInputScope(StepId),
    #[error("terminal step {0} is missing an input or output scope")]
    MissingTerminalScope(StepId),
    #[error("non-output step {0} unexpectedly has an output scope")]
    UnexpectedOutputScope(StepId),
    #[error("step {step} expected input scope {expected}, got {actual}")]
    InputScopeMismatch {
        step: StepId,
        expected: ScopeHash,
        actual: ScopeHash,
    },
    #[error("snapshot has {actual} steps but plan requires {expected}")]
    SnapshotStepCount { expected: usize, actual: usize },
    #[error("snapshot step identity mismatch: expected {expected}, got {actual}")]
    SnapshotStepId { expected: StepId, actual: StepId },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{Argv, EnvEdit, EnvKey, EnvPatch, EnvValue, FileModeMask, Process};

    const EXECUTION_ID: ExecutionId = ExecutionId(7);

    fn key(value: &str) -> EnvKey {
        EnvKey::new(value).unwrap()
    }

    fn value(value: &str) -> EnvValue {
        EnvValue::new(value).unwrap()
    }

    fn scope(path: &str) -> Scope {
        Scope::new(
            AbsolutePath::new(path).unwrap(),
            BTreeMap::from([(key("PATH"), value("/bin"))]),
            FileModeMask::new(0o022).unwrap(),
        )
    }

    fn run(program: &str) -> ExecutionPlan {
        let process = Process::new(Argv::new(program, Vec::new()).unwrap());
        ExecutionPlan::run(Pipeline::simple(process), IoMode::Captured)
    }

    fn env(name: &str, value: &str) -> ExecutionPlan {
        let patch = EnvPatch::new(BTreeMap::from([(key(name), EnvEdit::set(value).unwrap())]));
        ExecutionPlan::builtin(BuiltinCommand::env(patch).unwrap())
    }

    fn cd(path: &str) -> ExecutionPlan {
        ExecutionPlan::builtin(BuiltinCommand::cd(path).unwrap())
    }

    fn umask(mask: u16) -> ExecutionPlan {
        ExecutionPlan::builtin(BuiltinCommand::umask(FileModeMask::new(mask).unwrap()))
    }

    fn sequence(plans: Vec<ExecutionPlan>) -> ExecutionPlan {
        plans
            .into_iter()
            .reduce(|first, then| ExecutionPlan::sequence(first, then, SequenceCondition::Success))
            .unwrap()
    }

    fn execution(plan: ExecutionPlan, initial: &Scope) -> Execution {
        let spec = ExecutionSpec::new(initial.compute_hash(), plan).unwrap();
        Execution::new(EXECUTION_ID, spec)
    }

    fn step(index: u32) -> StepId {
        StepId {
            execution: EXECUTION_ID,
            index,
        }
    }

    fn ready_ids(transition: &ExecutionTransition) -> Vec<StepId> {
        transition.runtime_steps.iter().copied().collect()
    }

    fn cancel_ids(transition: &ExecutionTransition) -> Vec<StepId> {
        transition.runtime_steps.iter().copied().collect()
    }

    fn start(execution: &mut Execution, id: StepId) {
        assert!(execution.step(id).unwrap().state().is_active());
    }

    fn finish_run(
        execution: &mut Execution,
        id: StepId,
        result: Result<(), StepFailure>,
    ) -> ExecutionTransition {
        start(execution, id);
        execution
            .complete_run(
                id,
                match result {
                    Ok(()) => RunCompletion::Succeeded,
                    Err(failure) => RunCompletion::Failed(failure),
                },
            )
            .unwrap()
    }

    #[test]
    fn stable_step_ids_and_parallel_ready_order_follow_plan_preorder() {
        let initial = scope("/workspace");
        let plan = ExecutionPlan::sequence(
            env("MODE", "release"),
            ExecutionPlan::parallel(
                vec![run("left"), sequence(vec![run("middle"), run("right")])],
                ParallelJoin::All,
            )
            .unwrap(),
            SequenceCondition::Success,
        );
        let mut execution = execution(plan, &initial);

        assert_eq!(
            execution
                .steps()
                .iter()
                .map(StepRecord::id)
                .collect::<Vec<_>>(),
            vec![step(1), step(2), step(3), step(4)]
        );
        assert_eq!(ready_ids(&execution.advance().unwrap()), vec![step(1)]);

        start(&mut execution, step(1));
        let transition = execution
            .complete_builtin(step(1), &initial, Ok(BuiltinSuccess::Env))
            .unwrap();
        assert_eq!(ready_ids(&transition), vec![step(2), step(3)]);
    }

    #[test]
    fn sequence_threads_env_cwd_and_umask_while_runs_preserve_scope() {
        let initial = scope("/workspace");
        let plan = sequence(vec![
            env("MODE", "release"),
            run("compile"),
            cd("target"),
            umask(0o077),
            run("package"),
        ]);
        let mut execution = execution(plan, &initial);

        assert_eq!(ready_ids(&execution.advance().unwrap()), vec![step(1)]);
        start(&mut execution, step(1));
        let env_transition = execution
            .complete_builtin(step(1), &initial, Ok(BuiltinSuccess::Env))
            .unwrap();
        let env_scope = env_transition.new_scopes[0].clone();
        assert_eq!(
            env_scope.env().get(&key("MODE")).unwrap().as_str(),
            "release"
        );
        assert_eq!(
            execution.step(step(2)).unwrap().input_scope().unwrap(),
            env_scope.compute_hash()
        );

        let run_transition = finish_run(&mut execution, step(2), Ok(()));
        assert_eq!(*run_transition.runtime_steps.first().unwrap(), step(3));
        assert_eq!(
            execution.step(step(2)).unwrap().output_scope(),
            Some(env_scope.compute_hash())
        );

        start(&mut execution, step(3));
        let cd_transition = execution
            .complete_builtin(
                step(3),
                &env_scope,
                Ok(BuiltinSuccess::Cd {
                    cwd: AbsolutePath::new("/workspace/target").unwrap(),
                }),
            )
            .unwrap();
        let cwd_scope = cd_transition.new_scopes[0].clone();
        assert_eq!(
            cwd_scope.cwd().as_path().to_str(),
            Some("/workspace/target")
        );
        assert_eq!(
            execution.step(step(4)).unwrap().input_scope().unwrap(),
            cwd_scope.compute_hash()
        );

        start(&mut execution, step(4));
        let umask_transition = execution
            .complete_builtin(step(4), &cwd_scope, Ok(BuiltinSuccess::Umask))
            .unwrap();
        let final_scope = umask_transition.new_scopes[0].clone();
        assert_eq!(final_scope.umask().get(), 0o077);
        assert_eq!(
            execution.step(step(5)).unwrap().input_scope().unwrap(),
            final_scope.compute_hash()
        );

        finish_run(&mut execution, step(5), Ok(()));
        assert_eq!(execution.state(), ExecutionState::Succeeded);
    }

    #[test]
    fn successful_and_failed_conditions_skip_unselected_leaves() {
        let initial = scope("/workspace");
        let cases = [
            (SequenceCondition::Success, true, true),
            (SequenceCondition::Success, false, false),
            (SequenceCondition::Failure, true, false),
            (SequenceCondition::Failure, false, true),
        ];

        for (condition, first_succeeds, then_runs) in cases {
            let plan = ExecutionPlan::sequence(run("first"), run("then"), condition);
            let mut execution = execution(plan, &initial);
            execution.advance().unwrap();
            let result = if first_succeeds {
                Ok(())
            } else {
                Err(StepFailure::Exit { code: 2 })
            };
            let transition = finish_run(&mut execution, step(1), result);

            assert_eq!(ready_ids(&transition) == vec![step(2)], then_runs);
            if then_runs {
                finish_run(&mut execution, step(2), Ok(()));
                assert_eq!(execution.state(), ExecutionState::Succeeded);
            } else {
                assert_eq!(
                    execution.step(step(2)).unwrap().state(),
                    &StepState::Skipped {
                        reason: SkipReason::ConditionNotMet
                    }
                );
                assert_eq!(
                    execution.state(),
                    if first_succeeds {
                        ExecutionState::Succeeded
                    } else {
                        ExecutionState::Failed
                    }
                );
            }
        }
    }

    #[test]
    fn failure_recovery_receives_scope_produced_before_the_failure() {
        let initial = scope("/workspace");
        let first = sequence(vec![env("MODE", "release"), run("fail")]);
        let plan = ExecutionPlan::sequence(first, run("recover"), SequenceCondition::Failure);
        let mut execution = execution(plan, &initial);

        execution.advance().unwrap();
        start(&mut execution, step(1));
        let transition = execution
            .complete_builtin(step(1), &initial, Ok(BuiltinSuccess::Env))
            .unwrap();
        let changed_scope = transition.new_scopes[0].clone();
        let recovery = finish_run(&mut execution, step(2), Err(StepFailure::Exit { code: 1 }));
        assert_eq!(ready_ids(&recovery), vec![step(3)]);
        assert_eq!(
            execution.step(step(3)).unwrap().input_scope().unwrap(),
            changed_scope.compute_hash()
        );
        finish_run(&mut execution, step(3), Ok(()));
        assert_eq!(execution.state(), ExecutionState::Succeeded);
    }

    #[test]
    fn always_runs_cleanup_and_preserves_either_failure() {
        let initial = scope("/workspace");
        for first_succeeds in [true, false] {
            for cleanup_succeeds in [true, false] {
                let plan = ExecutionPlan::sequence(
                    run("first"),
                    run("cleanup"),
                    SequenceCondition::Always,
                );
                let mut execution = execution(plan, &initial);
                execution.advance().unwrap();
                let first = first_succeeds
                    .then_some(())
                    .ok_or(StepFailure::Exit { code: 1 });
                assert_eq!(
                    ready_ids(&finish_run(&mut execution, step(1), first)),
                    vec![step(2)]
                );
                let cleanup = cleanup_succeeds
                    .then_some(())
                    .ok_or(StepFailure::Exit { code: 2 });
                finish_run(&mut execution, step(2), cleanup);
                assert_eq!(
                    execution.state(),
                    if first_succeeds && cleanup_succeeds {
                        ExecutionState::Succeeded
                    } else {
                        ExecutionState::Failed
                    }
                );
            }
        }
    }

    #[test]
    fn parallel_all_forks_input_scope_waits_for_all_and_never_merges_scope() {
        let initial = scope("/workspace");
        let parallel = ExecutionPlan::parallel(
            vec![
                sequence(vec![env("LEFT", "1"), run("left")]),
                sequence(vec![cd("right"), run("right")]),
            ],
            ParallelJoin::All,
        )
        .unwrap();
        let plan = ExecutionPlan::sequence(parallel, run("after"), SequenceCondition::Success);
        let mut execution = execution(plan, &initial);

        let first = execution.advance().unwrap();
        assert_eq!(ready_ids(&first), vec![step(1), step(3)]);
        assert!(
            first.runtime_steps.iter().all(
                |id| execution.step(*id).unwrap().input_scope() == Some(initial.compute_hash())
            )
        );

        start(&mut execution, step(1));
        let left = execution
            .complete_builtin(step(1), &initial, Ok(BuiltinSuccess::Env))
            .unwrap();
        let left_scope = left.new_scopes[0].clone();
        start(&mut execution, step(3));
        let right = execution
            .complete_builtin(
                step(3),
                &initial,
                Ok(BuiltinSuccess::Cd {
                    cwd: AbsolutePath::new("/workspace/right").unwrap(),
                }),
            )
            .unwrap();
        let right_scope = right.new_scopes[0].clone();
        assert_ne!(left_scope.compute_hash(), right_scope.compute_hash());

        finish_run(&mut execution, step(2), Ok(()));
        assert_eq!(execution.state(), ExecutionState::Running);
        assert_eq!(
            ready_ids(&execution.advance().unwrap()),
            Vec::<StepId>::new(),
            "the outer continuation must wait for the other branch"
        );

        let completed = finish_run(&mut execution, step(4), Ok(()));
        assert_eq!(ready_ids(&completed), vec![step(5)]);
        assert_eq!(
            execution.step(step(5)).unwrap().input_scope().unwrap(),
            initial.compute_hash()
        );
        finish_run(&mut execution, step(5), Ok(()));
        assert_eq!(execution.state(), ExecutionState::Succeeded);
    }

    #[test]
    fn any_success_waits_for_running_loser_cancellation_before_becoming_terminal() {
        let initial = scope("/workspace");
        let slow = ExecutionPlan::sequence(
            run("slow-first"),
            run("slow-second"),
            SequenceCondition::Success,
        );
        let plan =
            ExecutionPlan::parallel(vec![run("fast"), slow], ParallelJoin::AnySuccess).unwrap();
        let mut execution = execution(plan, &initial);

        assert_eq!(
            ready_ids(&execution.advance().unwrap()),
            vec![step(1), step(2)]
        );
        start(&mut execution, step(1));
        start(&mut execution, step(2));
        let transition = execution
            .complete_run(step(1), RunCompletion::Succeeded)
            .unwrap();

        assert_eq!(cancel_ids(&transition), vec![step(2)]);
        assert_eq!(
            execution.step(step(2)).unwrap().state(),
            &StepState::Cancelling {
                cause: StepCancelCause::AnySuccessSatisfied,
                mode: CancelMode::Force,
            }
        );
        assert_eq!(
            execution.step(step(3)).unwrap().state(),
            &StepState::Skipped {
                reason: SkipReason::AnySuccessSatisfied
            }
        );
        assert_eq!(execution.state(), ExecutionState::Running);

        execution
            .complete_run(step(2), RunCompletion::Cancelled)
            .unwrap();
        assert_eq!(
            execution.step(step(2)).unwrap().state(),
            &StepState::Cancelled {
                cause: StepCancelCause::AnySuccessSatisfied
            }
        );
        assert_eq!(execution.state(), ExecutionState::Succeeded);
    }

    #[test]
    fn any_success_cancel_is_best_effort_and_loser_may_still_succeed() {
        let initial = scope("/workspace");
        let plan = ExecutionPlan::parallel(
            vec![run("winner"), run("racing-loser")],
            ParallelJoin::AnySuccess,
        )
        .unwrap();
        let mut execution = execution(plan, &initial);
        execution.advance().unwrap();
        start(&mut execution, step(1));
        start(&mut execution, step(2));

        execution
            .complete_run(step(1), RunCompletion::Succeeded)
            .unwrap();
        assert!(matches!(
            execution.step(step(2)).unwrap().state(),
            StepState::Cancelling { .. }
        ));

        execution
            .complete_run(step(2), RunCompletion::Succeeded)
            .unwrap();
        assert_eq!(
            execution.step(step(2)).unwrap().state(),
            &StepState::Succeeded
        );
        assert_eq!(execution.state(), ExecutionState::Succeeded);
    }

    #[test]
    fn any_success_fails_only_after_every_branch_is_terminal_without_success() {
        let initial = scope("/workspace");
        let plan = ExecutionPlan::parallel(vec![run("one"), run("two")], ParallelJoin::AnySuccess)
            .unwrap();
        let mut execution = execution(plan, &initial);
        execution.advance().unwrap();
        start(&mut execution, step(1));
        start(&mut execution, step(2));

        execution
            .complete_run(
                step(1),
                RunCompletion::Failed(StepFailure::Exit { code: 1 }),
            )
            .unwrap();
        assert_eq!(execution.state(), ExecutionState::Running);
        execution
            .complete_run(
                step(2),
                RunCompletion::Failed(StepFailure::Exit { code: 2 }),
            )
            .unwrap();
        assert_eq!(execution.state(), ExecutionState::Failed);
    }

    #[test]
    fn user_cancel_marks_pending_terminal_but_running_work_cancelling_until_completion() {
        let initial = scope("/workspace");
        let plan = ExecutionPlan::parallel(
            vec![run("one"), sequence(vec![run("two"), run("three")])],
            ParallelJoin::All,
        )
        .unwrap();
        let mut execution = execution(plan, &initial);
        execution.advance().unwrap();
        start(&mut execution, step(1));

        let transition = execution.cancel(CancelMode::Graceful);

        assert_eq!(cancel_ids(&transition), vec![step(1), step(2)]);
        assert_eq!(
            execution.step(step(1)).unwrap().state(),
            &StepState::Cancelling {
                cause: StepCancelCause::ExecutionRequested,
                mode: CancelMode::Graceful,
            }
        );
        assert_eq!(
            execution.step(step(2)).unwrap().state(),
            &StepState::Cancelling {
                cause: StepCancelCause::ExecutionRequested,
                mode: CancelMode::Graceful,
            }
        );
        execution.complete_cancelled(step(2)).unwrap();
        assert_eq!(
            execution.step(step(3)).unwrap().state(),
            &StepState::Cancelled {
                cause: StepCancelCause::ExecutionRequested,
            }
        );
        assert_eq!(execution.state(), ExecutionState::Cancelling);

        execution
            .complete_run(step(1), RunCompletion::Cancelled)
            .unwrap();
        assert_eq!(execution.state(), ExecutionState::Cancelled);
    }

    #[test]
    fn cancelled_single_run_can_still_finish_successfully() {
        let initial = scope("/workspace");
        let mut execution = execution(run("fast"), &initial);
        execution.advance().unwrap();
        start(&mut execution, step(1));

        let transition = execution.cancel(CancelMode::Graceful);
        assert_eq!(cancel_ids(&transition), vec![step(1)]);
        assert!(matches!(execution.state(), ExecutionState::Cancelling));

        execution
            .complete_run(step(1), RunCompletion::Succeeded)
            .unwrap();
        assert_eq!(execution.state(), ExecutionState::Succeeded);
        assert_eq!(
            execution.step(step(1)).unwrap().state(),
            &StepState::Succeeded
        );
    }

    #[test]
    fn cancellation_escalation_is_idempotent_and_force_strengthens_graceful() {
        let initial = scope("/workspace");
        let mut execution = execution(run("slow"), &initial);
        execution.advance().unwrap();
        start(&mut execution, step(1));

        let graceful = execution.cancel(CancelMode::Graceful);
        assert_eq!(graceful.runtime_steps.len(), 1);
        assert!(
            execution
                .cancel(CancelMode::Graceful)
                .runtime_steps
                .is_empty()
        );

        let force = execution.cancel(CancelMode::Force);
        assert_eq!(force.runtime_steps, BTreeSet::from([step(1)]));
        assert_eq!(
            execution.step(step(1)).unwrap().state(),
            &StepState::Cancelling {
                cause: StepCancelCause::ExecutionRequested,
                mode: CancelMode::Force,
            }
        );
        assert_eq!(execution.state(), ExecutionState::Cancelling);
    }

    #[test]
    fn reducer_input_order_is_deterministic_around_cancel_and_completion() {
        let initial = scope("/workspace");
        let mut completion_first = execution(run("fast"), &initial);
        completion_first.advance().unwrap();
        start(&mut completion_first, step(1));
        completion_first
            .complete_run(step(1), RunCompletion::Succeeded)
            .unwrap();
        assert!(
            completion_first
                .cancel(CancelMode::Graceful)
                .runtime_steps
                .is_empty()
        );
        assert_eq!(completion_first.state(), ExecutionState::Succeeded);

        let mut cancel_first = execution(run("fast"), &initial);
        cancel_first.advance().unwrap();
        start(&mut cancel_first, step(1));
        cancel_first.cancel(CancelMode::Graceful);
        cancel_first
            .complete_run(step(1), RunCompletion::Succeeded)
            .unwrap();
        assert_eq!(cancel_first.state(), ExecutionState::Succeeded);
    }

    #[test]
    fn runtime_cannot_report_cancelled_without_committed_cancel_intent() {
        let initial = scope("/workspace");
        let mut execution = execution(run("one"), &initial);
        execution.advance().unwrap();
        start(&mut execution, step(1));

        assert!(matches!(
            execution.complete_run(step(1), RunCompletion::Cancelled),
            Err(ExecutionError::UnexpectedRunCancellation(id)) if id == step(1)
        ));
        assert_eq!(
            execution.step(step(1)).unwrap().state(),
            &StepState::Running
        );
    }

    #[test]
    fn builtin_completion_validates_action_success_kind_and_exact_input_scope() {
        let initial = scope("/workspace");
        let other = scope("/other");
        let mut execution = execution(env("MODE", "release"), &initial);
        execution.advance().unwrap();
        start(&mut execution, step(1));

        assert!(matches!(
            execution.complete_builtin(step(1), &other, Ok(BuiltinSuccess::Env)),
            Err(ExecutionError::InputScopeMismatch { .. })
        ));
        assert_eq!(
            execution.step(step(1)).unwrap().state(),
            &StepState::Running
        );
        assert!(matches!(
            execution.complete_builtin(
                step(1),
                &initial,
                Ok(BuiltinSuccess::Cd {
                    cwd: AbsolutePath::new("/other").unwrap()
                })
            ),
            Err(ExecutionError::WrongBuiltinSuccess { .. })
        ));
        assert_eq!(
            execution.step(step(1)).unwrap().state(),
            &StepState::Running
        );
    }

    #[test]
    fn snapshot_restore_and_restart_interruption_preserve_reducer_semantics() {
        let initial = scope("/workspace");
        let plan =
            ExecutionPlan::sequence(run("primary"), run("recover"), SequenceCondition::Failure);
        let mut execution = execution(plan, &initial);
        execution.advance().unwrap();
        start(&mut execution, step(1));
        execution.cancel(CancelMode::Graceful);

        let mut restored = Execution::restore(execution.snapshot()).unwrap();
        restored.interrupt_running("daemon restarted");
        assert!(matches!(
            restored.step(step(1)).unwrap().state(),
            StepState::Failed {
                failure: StepFailure::Infrastructure { .. }
            }
        ));
        assert_eq!(
            restored.state(),
            ExecutionState::Cancelled,
            "the pending recovery branch was cancelled by the user request before restart"
        );
    }

    #[test]
    fn restart_interruption_without_user_cancel_can_select_failure_recovery() {
        let initial = scope("/workspace");
        let plan =
            ExecutionPlan::sequence(run("primary"), run("recover"), SequenceCondition::Failure);
        let mut execution = execution(plan, &initial);
        execution.advance().unwrap();
        start(&mut execution, step(1));

        let mut restored = Execution::restore(execution.snapshot()).unwrap();
        restored.interrupt_running("daemon restarted");
        let transition = restored.advance().unwrap();

        assert_eq!(ready_ids(&transition), vec![step(2)]);
        assert_eq!(
            restored.step(step(2)).unwrap().input_scope(),
            Some(initial.compute_hash())
        );
    }

    #[test]
    fn restore_rejects_forged_step_identity_and_terminal_scope_state() {
        let initial = scope("/workspace");
        let execution = execution(run("one"), &initial);
        let mut wrong_id = execution.snapshot();
        wrong_id.steps[0].id.index = 2;
        assert!(matches!(
            Execution::restore(wrong_id),
            Err(ExecutionError::SnapshotStepId { .. })
        ));

        let mut missing_scope = execution.snapshot();
        missing_scope.steps[0].state = StepState::Succeeded;
        assert!(matches!(
            Execution::restore(missing_scope),
            Err(ExecutionError::MissingTerminalScope(_))
        ));

        let mut cancelling_without_scope = execution.snapshot();
        cancelling_without_scope.steps[0].state = StepState::Cancelling {
            cause: StepCancelCause::ExecutionRequested,
            mode: CancelMode::Graceful,
        };
        assert!(matches!(
            Execution::restore(cancelling_without_scope),
            Err(ExecutionError::MissingInputScope(_))
        ));
    }

    #[test]
    fn failed_reducer_transitions_do_not_partially_mutate_durable_state() {
        let initial = scope("/workspace");
        let other_hash = scope("/other").compute_hash();
        let parallel =
            ExecutionPlan::parallel(vec![run("one"), run("two")], ParallelJoin::All).unwrap();
        let mut forged = execution(parallel, &initial).snapshot();
        forged.steps[1].input_scope = Some(other_hash);
        assert!(Execution::restore(forged.clone()).is_err());
        let mut restored = Execution {
            id: forged.id,
            spec: forged.spec,
            steps: forged.steps,
            cancel_requested: forged.cancel_requested,
        };

        assert!(matches!(
            restored.advance(),
            Err(ExecutionError::InputScopeMismatch { step: id, .. }) if id == step(2)
        ));
        assert_eq!(restored.step(step(1)).unwrap().input_scope(), None);

        let plan = ExecutionPlan::sequence(run("first"), run("then"), SequenceCondition::Success);
        let mut execution = execution(plan, &initial);
        execution.advance().unwrap();
        start(&mut execution, step(1));
        let mut forged = execution.snapshot();
        forged.steps[1].input_scope = Some(other_hash);
        assert!(Execution::restore(forged.clone()).is_err());
        let mut restored = Execution {
            id: forged.id,
            spec: forged.spec,
            steps: forged.steps,
            cancel_requested: forged.cancel_requested,
        };

        assert!(matches!(
            restored.complete_run(step(1), RunCompletion::Succeeded),
            Err(ExecutionError::InputScopeMismatch { step: id, .. }) if id == step(2)
        ));
        assert_eq!(restored.step(step(1)).unwrap().state(), &StepState::Running);
        assert_eq!(restored.step(step(1)).unwrap().output_scope(), None);
    }
    #[test]
    fn ready_atomically_activates_builtin_and_run_and_does_not_repeat() {
        let initial = scope("/workspace");
        let plan =
            ExecutionPlan::parallel(vec![env("A", "1"), run("work")], ParallelJoin::All).unwrap();
        let mut execution = execution(plan, &initial);
        let transition = execution.advance().unwrap();
        assert_eq!(transition.runtime_steps, BTreeSet::from([step(1), step(2)]));
        for id in transition.runtime_steps {
            let record = execution.step(id).unwrap();
            assert_eq!(record.state(), &StepState::Running);
            assert_eq!(record.input_scope(), Some(initial.compute_hash()));
            assert_eq!(record.output_scope(), None);
        }
        assert!(execution.advance().unwrap().runtime_steps.is_empty());
        assert_eq!(Execution::restore(execution.snapshot()).unwrap(), execution);
        let cancelled = execution.cancel(CancelMode::Force);
        assert_eq!(cancelled.runtime_steps, BTreeSet::from([step(1), step(2)]));
        execution.complete_cancelled(step(1)).unwrap();
        execution.complete_cancelled(step(2)).unwrap();
        assert_eq!(execution.state(), ExecutionState::Cancelled);
        assert!(
            execution
                .steps()
                .iter()
                .all(|step| step.output_scope().is_none())
        );
    }

    #[test]
    fn cancel_cause_is_first_wins_and_mode_is_monotone_in_both_orders() {
        let initial = scope("/workspace");
        for execution_first in [true, false] {
            let mut execution = execution(
                ExecutionPlan::parallel(
                    vec![run("winner"), run("loser")],
                    ParallelJoin::AnySuccess,
                )
                .unwrap(),
                &initial,
            );
            execution.advance().unwrap();
            if execution_first {
                execution.cancel(CancelMode::Graceful);
            }
            let transition = execution
                .complete_run(step(1), RunCompletion::Succeeded)
                .unwrap();
            assert_eq!(transition.runtime_steps, BTreeSet::from([step(2)]));
            if !execution_first {
                execution.cancel(CancelMode::Graceful);
            }
            let cause = if execution_first {
                StepCancelCause::ExecutionRequested
            } else {
                StepCancelCause::AnySuccessSatisfied
            };
            assert_eq!(
                execution.step(step(2)).unwrap().state(),
                &StepState::Cancelling {
                    cause,
                    mode: CancelMode::Force
                }
            );
            assert!(
                execution
                    .cancel(CancelMode::Graceful)
                    .runtime_steps
                    .is_empty()
            );
            execution
                .complete_run(
                    step(2),
                    RunCompletion::Failed(StepFailure::Exit { code: 7 }),
                )
                .unwrap();
            assert_eq!(execution.state(), ExecutionState::Succeeded);
            assert_eq!(Execution::restore(execution.snapshot()).unwrap(), execution);
        }
    }

    #[test]
    fn excluded_loser_never_starts_its_always_or_failure_continuation() {
        let initial = scope("/workspace");
        for when in [
            SequenceCondition::Success,
            SequenceCondition::Failure,
            SequenceCondition::Always,
        ] {
            for failed in [true, false] {
                let loser = ExecutionPlan::sequence(run("loser"), run("must-not-run"), when);
                let parallel =
                    ExecutionPlan::parallel(vec![run("winner"), loser], ParallelJoin::AnySuccess)
                        .unwrap();
                let mut execution = execution(
                    ExecutionPlan::sequence(
                        parallel,
                        run("after-drain"),
                        SequenceCondition::Success,
                    ),
                    &initial,
                );
                execution.advance().unwrap();
                execution
                    .complete_run(step(1), RunCompletion::Succeeded)
                    .unwrap();
                assert_eq!(
                    execution.step(step(4)).unwrap().state(),
                    &StepState::Pending
                );
                let result = if failed {
                    RunCompletion::Failed(StepFailure::Exit { code: 1 })
                } else {
                    RunCompletion::Succeeded
                };
                let next = execution.complete_run(step(2), result).unwrap();
                assert_eq!(next.runtime_steps, BTreeSet::from([step(4)]));
                assert_eq!(
                    execution.step(step(3)).unwrap().state(),
                    &StepState::Skipped {
                        reason: SkipReason::AnySuccessSatisfied
                    }
                );
                assert_eq!(execution.step(step(3)).unwrap().input_scope(), None);
                assert_eq!(Execution::restore(execution.snapshot()).unwrap(), execution);
            }
        }
    }

    #[test]
    fn execution_cancellation_excludes_all_sequence_conditions_even_after_natural_completion() {
        let initial = scope("/workspace");
        for when in [
            SequenceCondition::Success,
            SequenceCondition::Failure,
            SequenceCondition::Always,
        ] {
            let mut execution = execution(
                ExecutionPlan::sequence(run("work"), run("excluded"), when),
                &initial,
            );
            execution.advance().unwrap();
            execution.cancel(CancelMode::Graceful);
            assert!(
                execution
                    .complete_run(step(1), RunCompletion::Succeeded)
                    .unwrap()
                    .runtime_steps
                    .is_empty()
            );
            assert_eq!(execution.step(step(2)).unwrap().input_scope(), None);
            assert!(execution.state().is_terminal());
        }
    }

    #[test]
    fn restore_rejects_forged_scope_flow_and_early_continuations() {
        let initial = scope("/workspace");
        let mut execution = execution(sequence(vec![run("first"), run("then")]), &initial);
        execution.advance().unwrap();
        let mut snapshot = execution.snapshot();
        snapshot.steps[1].state = StepState::Running;
        snapshot.steps[1].input_scope = Some(initial.compute_hash());
        assert!(Execution::restore(snapshot).is_err());
        let mut snapshot = execution.snapshot();
        snapshot.steps[0].input_scope = Some(scope("/other").compute_hash());
        assert!(Execution::restore(snapshot).is_err());
        let mut snapshot = execution.snapshot();
        snapshot.cancel_requested = Some(CancelMode::Force);
        assert!(Execution::restore(snapshot).is_err());
        let before = execution.snapshot();
        assert!(
            execution
                .complete_run(
                    step(1),
                    RunCompletion::Failed(StepFailure::Exit { code: 0 })
                )
                .is_err()
        );
        assert_eq!(execution.snapshot(), before);
    }
}
