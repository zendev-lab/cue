//! Non-interactive `.cue` file runner for IPC v4.

use std::io::Write as _;
use std::path::PathBuf;

use anyhow::{Context as _, Result};
use cue_core::vnext::{ExecutionState, OutputStream, StepFailure, StepState};
use cue_protocol::{OutputRange, Query, ResultPayload};

use crate::default_socket_path;
use crate::vnext::{VnextClient, output_bytes, process_scope, wait_execution};

pub fn run(path: PathBuf) -> Result<i32> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build client runtime")?;
    runtime.block_on(run_async(path))
}

async fn run_async(path: PathBuf) -> Result<i32> {
    let source = std::fs::read_to_string(&path)
        .with_context(|| format!("read Cue file {}", path.display()))?;
    let socket = std::env::var_os("CUE_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(default_socket_path);
    let mut client = VnextClient::connect(&socket).await?;
    let submitted = client
        .submit_file(process_scope()?, &source)
        .await
        .with_context(|| format!("submit Cue file {}", path.display()))?;
    let execution = wait_execution(&mut client, submitted.snapshot.id).await?;
    write_execution_output(&mut client, &execution).await?;
    Ok(execution_exit_code(&execution))
}

pub(crate) async fn write_execution_output(
    client: &mut VnextClient,
    execution: &cue_protocol::ExecutionView,
) -> Result<()> {
    for step in &execution.snapshot.steps {
        let response = client
            .query(Query::ReadOutput {
                step: step.id(),
                stdout: full_range(),
                stderr: full_range(),
                terminal: full_range(),
            })
            .await?;
        let ResultPayload::Output { chunks } = response else {
            continue;
        };
        std::io::stdout().write_all(&output_bytes(&chunks, OutputStream::Stdout))?;
        std::io::stderr().write_all(&output_bytes(&chunks, OutputStream::Stderr))?;
        std::io::stdout().write_all(&output_bytes(&chunks, OutputStream::Terminal))?;
    }
    Ok(())
}

fn full_range() -> OutputRange {
    OutputRange {
        offset: 0,
        max_bytes: 16 * 1024 * 1024,
    }
}

pub(crate) fn execution_exit_code(execution: &cue_protocol::ExecutionView) -> i32 {
    match execution.state {
        ExecutionState::Succeeded => 0,
        ExecutionState::Failed => execution
            .snapshot
            .steps
            .iter()
            .find_map(|step| match step.state() {
                StepState::Failed {
                    failure: StepFailure::Exit { code },
                } => Some(*code),
                StepState::Failed { .. } => Some(1),
                _ => None,
            })
            .unwrap_or(1),
        ExecutionState::Cancelled { .. } => 130,
        ExecutionState::Pending | ExecutionState::Running => 1,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cue_core::ExecutionId;
    use cue_core::vnext::{
        AbsolutePath, Argv, Execution, ExecutionPlan, ExecutionSpec, FileModeMask, IoMode,
        Pipeline, Process, Scope,
    };

    use super::*;

    #[test]
    fn nonterminal_execution_maps_to_failure() {
        let scope = Scope::new(
            AbsolutePath::new("/tmp").unwrap(),
            BTreeMap::new(),
            FileModeMask::new(0o022).unwrap(),
        );
        let spec = ExecutionSpec::new(
            scope.compute_hash(),
            ExecutionPlan::run(
                Pipeline::simple(Process::new(Argv::new("true", Vec::new()).unwrap())),
                IoMode::Captured,
            ),
        )
        .unwrap();
        let execution = Execution::new(ExecutionId(1), spec);
        let view = cue_protocol::ExecutionView {
            snapshot: execution.snapshot(),
            state: execution.state(),
            created_at_ms: 1,
            updated_at_ms: 1,
        };
        assert_eq!(execution_exit_code(&view), 1);
    }
}
