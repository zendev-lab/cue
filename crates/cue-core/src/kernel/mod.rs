//! Cue's closed execution semantics.
//!
//! This module contains no transport, storage, parser, scheduler, session,
//! resource, or extension implementation types.

mod env;
mod execution;
mod fact;
mod plan;
mod process;
mod scope;

pub use env::{Env, EnvEdit, EnvKey, EnvPatch, EnvValue};
pub use execution::{
    BuiltinSuccess, CancelMode, Execution, ExecutionCancelReason, ExecutionError,
    ExecutionSnapshot, ExecutionState, ExecutionTransition, ReadyStep, SkipReason, StepAction,
    StepCancelReason, StepFailure, StepRecord, StepState,
};
pub use fact::{ExecutionProjection, Fact, FactDraft, FactEvent, OutputStream};
pub use plan::{
    BuiltinCommand, CdPath, EnvMutation, ExecutionPlan, ExecutionSpec, IoMode, ParallelBranches,
    ParallelJoin, PlanValidationError, SequenceCondition, StepDescriptor, StepKind,
};
pub use process::{Argv, PipeContinuation, PipeLink, Pipeline, Process, ProcessError};
pub use scope::{AbsolutePath, FileModeMask, Scope, ScopeError};
