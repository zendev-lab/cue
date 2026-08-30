use cue_core::{Execution, ExecutionProjection, Fact, FactDraft, StepState};

use crate::{RuntimeError, RuntimeErrorKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryCommit {
    pub execution: ExecutionProjection,
    pub facts: Vec<FactDraft>,
}

/// Convert every pre-restart Running Step into a durable infrastructure
/// failure. Pending work is not advanced here: the coordinator must persist
/// this recovery commit first, then run the normal advance/mark-running path.
pub fn recover_interrupted(
    previous: &ExecutionProjection,
    occurred_at_ms: i64,
    message: impl Into<String>,
) -> Result<Option<RecoveryCommit>, RuntimeError> {
    if occurred_at_ms < previous.updated_at_ms {
        return Err(RuntimeError::new(
            RuntimeErrorKind::InvalidInput,
            "recovery timestamp predates the stored execution",
        ));
    }
    let mut execution = Execution::restore(previous.snapshot.clone()).map_err(|error| {
        RuntimeError::new(
            RuntimeErrorKind::Conflict,
            format!("invalid snapshot: {error}"),
        )
    })?;
    if execution.state() != previous.state {
        return Err(RuntimeError::new(
            RuntimeErrorKind::Conflict,
            "stored aggregate state disagrees with the reducer snapshot",
        ));
    }
    if !execution
        .steps()
        .iter()
        .any(|step| matches!(step.state(), StepState::Running))
    {
        return Ok(None);
    }

    let before_steps = execution.steps().to_vec();
    let before_state = execution.state();
    execution.interrupt_running(message);
    let after_state = execution.state();
    let mut facts = Vec::new();
    for (before, after) in before_steps.iter().zip(execution.steps()) {
        if before != after {
            facts.push(FactDraft {
                occurred_at_ms,
                fact: Fact::StepStateChanged {
                    id: after.id(),
                    previous: before.state().clone(),
                    next: after.state().clone(),
                    input_scope: after.input_scope(),
                    output_scope: after.output_scope(),
                },
            });
        }
    }
    if before_state != after_state {
        facts.push(FactDraft {
            occurred_at_ms,
            fact: Fact::ExecutionStateChanged {
                id: execution.id(),
                previous: before_state,
                next: after_state.clone(),
            },
        });
    }
    if after_state.is_terminal() {
        facts.push(FactDraft {
            occurred_at_ms,
            fact: Fact::ExecutionFinished {
                id: execution.id(),
                state: after_state.clone(),
            },
        });
    }
    Ok(Some(RecoveryCommit {
        execution: ExecutionProjection {
            snapshot: execution.snapshot(),
            state: after_state,
            created_at_ms: previous.created_at_ms,
            updated_at_ms: occurred_at_ms,
        },
        facts,
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cue_core::{
        AbsolutePath, Argv, ExecutionPlan, ExecutionSpec, FileModeMask, IoMode, Pipeline, Process,
        Scope, StepFailure,
    };
    use cue_core::{ExecutionId, StepId};

    use super::*;

    fn running_projection() -> ExecutionProjection {
        let scope = Scope::new(
            AbsolutePath::new("/workspace").unwrap(),
            BTreeMap::new(),
            FileModeMask::new(0o022).unwrap(),
        );
        let plan = ExecutionPlan::run(
            Pipeline::simple(Process::new(Argv::new("true", Vec::new()).unwrap())),
            IoMode::Captured,
        );
        let mut execution = Execution::new(
            ExecutionId(1),
            ExecutionSpec::new(scope.compute_hash(), plan).unwrap(),
        );
        execution.advance().unwrap();
        execution
            .mark_running(StepId {
                execution: ExecutionId(1),
                index: 1,
            })
            .unwrap();
        ExecutionProjection {
            snapshot: execution.snapshot(),
            state: execution.state(),
            created_at_ms: 10,
            updated_at_ms: 11,
        }
    }

    #[test]
    fn recovery_fails_running_work_before_normal_advancement_resumes() {
        let recovered = recover_interrupted(&running_projection(), 20, "daemon restarted")
            .unwrap()
            .unwrap();
        assert_eq!(recovered.execution.state, cue_core::ExecutionState::Failed);
        assert_eq!(recovered.facts.len(), 3);
        assert!(matches!(
            &recovered.execution.snapshot.steps[0].state(),
            StepState::Failed {
                failure: StepFailure::Infrastructure { message }
            } if message == "daemon restarted"
        ));
    }

    #[test]
    fn recovery_is_a_noop_without_running_steps() {
        let mut projection = running_projection();
        let execution = Execution::new(projection.snapshot.id, projection.snapshot.spec.clone());
        projection.snapshot = execution.snapshot();
        projection.state = execution.state();
        assert!(
            recover_interrupted(&projection, 20, "restart")
                .unwrap()
                .is_none()
        );
    }
}
