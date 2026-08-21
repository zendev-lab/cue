//! DSL-to-runtime compilation.
//!
//! The parser still exposes a rich frontend command enum internally, but this
//! module is the only public bridge from text to the daemon's typed execution
//! contract. `cued` never depends on this crate.

use std::collections::BTreeMap;
use std::path::PathBuf;

use cue_core::command::{ModeParams, ParamValue};
use cue_core::cron::CronSchedule;
use cue_core::execution::{ExecutionPlan, ExecutionSpec, LaunchContext, SourceMetadata};
use cue_core::job::{SandboxMode, SandboxSettings, SandboxUpper};
use cue_core::mode::Mode;
use cue_core::pipeline::{ChainNode, JobPlan, ParallelOp, SerialOp};
use cue_core::scope::EnvDelta;

use crate::{ParseError, ResolvedCommand, parse_command, parse_file_script_command};

#[derive(Debug, Clone)]
pub enum CompiledCommand {
    Submit(ExecutionSpec),
    CreateSchedule {
        schedule: CronSchedule,
        execution: ExecutionSpec,
    },
    ApplyScopeDelta(EnvDelta),
    /// Connection-local commands and typed queries remain frontend actions.
    Frontend(ResolvedCommand),
}

#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error("{0}")]
    Invalid(String),
}

pub fn compile_command(
    input: &str,
    mode: Mode,
    source_name: impl Into<String>,
) -> Result<CompiledCommand, CompileError> {
    compile_resolved(parse_command(input, mode)?, source_name.into())
}

pub fn compile_file(
    input: &str,
    source_name: impl Into<String>,
) -> Result<ExecutionSpec, CompileError> {
    let source_name = source_name.into();
    match compile_resolved(parse_file_script_command(input)?, source_name)? {
        CompiledCommand::Submit(spec) => Ok(spec),
        _ => Err(CompileError::Invalid(
            "a .cue file must compile to one execution".into(),
        )),
    }
}

fn compile_resolved(
    command: ResolvedCommand,
    source_name: String,
) -> Result<CompiledCommand, CompileError> {
    match command {
        ResolvedCommand::Run { chain, params } => {
            let (plan, launch_context) = compile_run(chain, &params)?;
            Ok(CompiledCommand::Submit(execution_spec(
                plan,
                launch_context,
                source_name,
            )))
        }
        ResolvedCommand::Script { items, .. } => {
            if items.is_empty() {
                return Err(CompileError::Invalid("a .cue script is empty".into()));
            }
            let mut plans = Vec::with_capacity(items.len());
            let mut launch_context = None;
            for item in items {
                let (plan, item_launch) = compile_script_item(*item.command)?;
                if let Some(existing) = &launch_context {
                    if existing != &item_launch {
                        return Err(CompileError::Invalid(
                            ".cue files require one shared launch context; split commands with different pty, resource, wrapper, or workspace-view settings into separate submissions"
                                .into(),
                        ));
                    }
                } else {
                    launch_context = Some(item_launch);
                }
                plans.push(plan);
            }
            let plan = plans
                .into_iter()
                .reduce(|left, right| ExecutionPlan::OnSuccess {
                    left: Box::new(left),
                    right: Box::new(right),
                })
                .expect("non-empty script checked above");
            Ok(CompiledCommand::Submit(execution_spec(
                plan,
                launch_context.unwrap_or_default(),
                source_name,
            )))
        }
        ResolvedCommand::Cron {
            schedule,
            chain,
            params,
        } => {
            let (plan, launch_context) = compile_run(chain, &params)?;
            if launch_context.spawn_adapter.is_some() {
                return Err(CompileError::Invalid(
                    "scheduled executions cannot carry an ephemeral spawn adapter".into(),
                ));
            }
            Ok(CompiledCommand::CreateSchedule {
                schedule,
                execution: execution_spec(plan, launch_context, source_name),
            })
        }
        ResolvedCommand::Cd { path } => Ok(CompiledCommand::ApplyScopeDelta(EnvDelta {
            set: BTreeMap::new(),
            unset: Vec::new(),
            cwd: Some(PathBuf::from(path)),
        })),
        ResolvedCommand::Env { subcommand } => match compile_env_delta(subcommand.as_deref())? {
            Some(delta) => Ok(CompiledCommand::ApplyScopeDelta(delta)),
            None => Ok(CompiledCommand::Frontend(ResolvedCommand::Env {
                subcommand,
            })),
        },
        command => Ok(CompiledCommand::Frontend(command)),
    }
}

fn execution_spec(
    plan: ExecutionPlan,
    launch_context: LaunchContext,
    source_name: String,
) -> ExecutionSpec {
    ExecutionSpec {
        plan,
        start_scope: None,
        launch_context,
        source: Some(SourceMetadata {
            name: source_name,
            line: None,
            column: None,
        }),
        retry_of: None,
    }
}

fn compile_script_item(
    command: ResolvedCommand,
) -> Result<(ExecutionPlan, LaunchContext), CompileError> {
    match command {
        ResolvedCommand::Run { chain, params } => compile_run(chain, &params),
        ResolvedCommand::Cd { path } => Ok((
            ExecutionPlan::ContextDelta {
                delta: EnvDelta {
                    set: BTreeMap::new(),
                    unset: Vec::new(),
                    cwd: Some(PathBuf::from(path)),
                },
            },
            LaunchContext::default(),
        )),
        ResolvedCommand::Env { subcommand } => {
            let delta = compile_env_delta(subcommand.as_deref())?.ok_or_else(|| {
                CompileError::Invalid("`:env` without set/unset is not executable in a .cue file".into())
            })?;
            Ok((
                ExecutionPlan::ContextDelta { delta },
                LaunchContext::default(),
            ))
        }
        _ => Err(CompileError::Invalid(
            "a .cue file may contain runs, `:cd`, and `:env set|unset`; UI and query commands are frontend-only"
                .into(),
        )),
    }
}

fn compile_run(
    chain: ChainNode,
    params: &ModeParams,
) -> Result<(ExecutionPlan, LaunchContext), CompileError> {
    let scope_enabled = params.scope().unwrap_or(false);
    let mut plan = compile_chain(chain, scope_enabled)?;
    if let Some(cwd) = params.cwd() {
        plan = ExecutionPlan::OnSuccess {
            left: Box::new(ExecutionPlan::ContextDelta {
                delta: EnvDelta {
                    set: BTreeMap::new(),
                    unset: Vec::new(),
                    cwd: Some(cwd),
                },
            }),
            right: Box::new(plan),
        };
    }
    Ok((
        plan,
        LaunchContext {
            pty: match params.get("pty") {
                Some(ParamValue::Bool(value)) => Some(*value),
                _ => None,
            },
            needs: params.needs(),
            workspace_view: compile_workspace_view(params)?,
            wrapper_enabled: params.wrapper_enabled(),
            spawn_adapter: None,
        },
    ))
}

fn compile_chain(node: ChainNode, scope_enabled: bool) -> Result<ExecutionPlan, CompileError> {
    match node {
        ChainNode::Leaf(plan) => compile_job_plan(plan, scope_enabled),
        ChainNode::Serial { left, op, right } => {
            let left = Box::new(compile_chain(*left, scope_enabled)?);
            let right = Box::new(compile_chain(*right, scope_enabled)?);
            Ok(match op {
                SerialOp::Then => ExecutionPlan::OnSuccess { left, right },
                SerialOp::Always => ExecutionPlan::Always { left, right },
            })
        }
        ChainNode::Parallel { left, op, right } => {
            let branches = vec![
                compile_chain(*left, scope_enabled)?,
                compile_chain(*right, scope_enabled)?,
            ];
            Ok(match op {
                ParallelOp::All => ExecutionPlan::ParallelAll { branches },
                ParallelOp::Race => ExecutionPlan::AnySuccess { branches },
            })
        }
    }
}

fn compile_job_plan(plan: JobPlan, scope_enabled: bool) -> Result<ExecutionPlan, CompileError> {
    match plan {
        JobPlan::Pipeline(pipeline) => {
            if scope_enabled && pipeline.segments.len() == 1 {
                let segment = &pipeline.segments[0];
                if segment.env.is_empty()
                    && segment.pipe_to_next.is_none()
                    && let Some(delta) = compile_scope_command(&segment.command)?
                {
                    return Ok(ExecutionPlan::ContextDelta { delta });
                }
            }
            Ok(ExecutionPlan::Pipeline { pipeline })
        }
        JobPlan::And { left, right } => Ok(ExecutionPlan::OnSuccess {
            left: Box::new(compile_job_plan(*left, scope_enabled)?),
            right: Box::new(compile_job_plan(*right, scope_enabled)?),
        }),
        JobPlan::Or { left, right } => Ok(ExecutionPlan::OnFailure {
            left: Box::new(compile_job_plan(*left, scope_enabled)?),
            right: Box::new(compile_job_plan(*right, scope_enabled)?),
        }),
    }
}

fn compile_scope_command(words: &[String]) -> Result<Option<EnvDelta>, CompileError> {
    match words {
        [command, path] if command == "cd" => Ok(Some(EnvDelta {
            set: BTreeMap::new(),
            unset: Vec::new(),
            cwd: Some(PathBuf::from(path)),
        })),
        [command, subcommand, rest @ ..] if command == "env" && subcommand == "set" => {
            let text = format!("set {}", rest.join(" "));
            compile_env_delta(Some(&text))
        }
        [command, subcommand, rest @ ..] if command == "env" && subcommand == "unset" => {
            let text = format!("unset {}", rest.join(" "));
            compile_env_delta(Some(&text))
        }
        _ => Ok(None),
    }
}

fn compile_env_delta(subcommand: Option<&str>) -> Result<Option<EnvDelta>, CompileError> {
    let Some(subcommand) = subcommand.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if let Some(assignments) = subcommand.strip_prefix("set ") {
        let mut set = BTreeMap::new();
        for assignment in assignments.split_whitespace() {
            let Some((key, value)) = assignment.split_once('=') else {
                return Err(CompileError::Invalid(format!(
                    "`:env set` expects KEY=VALUE, got `{assignment}`"
                )));
            };
            validate_env_name(key)?;
            set.insert(key.to_string(), value.to_string());
        }
        if set.is_empty() {
            return Err(CompileError::Invalid(
                "`:env set` requires at least one KEY=VALUE assignment".into(),
            ));
        }
        return Ok(Some(EnvDelta {
            set,
            unset: Vec::new(),
            cwd: None,
        }));
    }
    if let Some(keys) = subcommand.strip_prefix("unset ") {
        let unset = keys
            .split_whitespace()
            .map(|key| {
                validate_env_name(key)?;
                Ok(key.to_string())
            })
            .collect::<Result<Vec<_>, CompileError>>()?;
        if unset.is_empty() {
            return Err(CompileError::Invalid(
                "`:env unset` requires at least one variable name".into(),
            ));
        }
        return Ok(Some(EnvDelta {
            set: BTreeMap::new(),
            unset,
            cwd: None,
        }));
    }
    Err(CompileError::Invalid(
        "`:env` supports `set KEY=VALUE` and `unset KEY` for mutations".into(),
    ))
}

fn validate_env_name(name: &str) -> Result<(), CompileError> {
    let mut chars = name.chars();
    if !chars
        .next()
        .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
        || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Err(CompileError::Invalid(format!(
            "invalid environment variable name `{name}`"
        )));
    }
    Ok(())
}

fn compile_workspace_view(params: &ModeParams) -> Result<Option<SandboxSettings>, CompileError> {
    let mode = match params.get("sandbox") {
        None => {
            if params.get("sandbox.upper").is_some() {
                return Err(CompileError::Invalid(
                    "sandbox.upper requires sandbox=overlay".into(),
                ));
            }
            return Ok(None);
        }
        Some(ParamValue::Str(value)) if value == "overlay" => SandboxMode::Overlay,
        Some(ParamValue::Str(value)) => {
            return Err(CompileError::Invalid(format!(
                "unsupported workspace view `{value}`; supported value: overlay"
            )));
        }
        Some(ParamValue::Bool(_)) => {
            return Err(CompileError::Invalid(
                "sandbox expects a string value".into(),
            ));
        }
    };
    let upper = match params.get("sandbox.upper") {
        None => None,
        Some(ParamValue::Str(value)) if value == "tmpfs" => Some(SandboxUpper::Tmpfs),
        Some(ParamValue::Str(value)) => Some(SandboxUpper::Directory(PathBuf::from(value))),
        Some(ParamValue::Bool(_)) => {
            return Err(CompileError::Invalid(
                "sandbox.upper expects a string value".into(),
            ));
        }
    };
    Ok(Some(SandboxSettings { mode, upper }))
}

#[cfg(test)]
mod tests {
    use cue_core::execution::ExecutionPlan;

    use super::*;

    fn compiled(input: &str) -> ExecutionSpec {
        match compile_command(input, Mode::Job, "<test>").unwrap() {
            CompiledCommand::Submit(spec) => spec,
            other => panic!("expected execution, got {other:?}"),
        }
    }

    #[test]
    fn compiles_every_execution_operator_without_a_bridge_state() {
        let spec = compiled("false || echo recover && echo next -> echo serial ~> echo always");
        assert!(matches!(spec.plan, ExecutionPlan::Always { .. }));

        let all = compiled("echo left ||| echo right");
        assert!(matches!(all.plan, ExecutionPlan::ParallelAll { .. }));
        let any = compiled("false |?| true");
        assert!(matches!(any.plan, ExecutionPlan::AnySuccess { .. }));
    }

    #[test]
    fn leading_assignments_remain_process_local_in_typed_segments() {
        let spec = compiled("ONE=1 EMPTY= sh -c 'printf %s $ONE'");
        let ExecutionPlan::Pipeline { pipeline } = spec.plan else {
            panic!("expected pipeline")
        };
        assert_eq!(pipeline.segments[0].env.get("ONE"), Some(&"1".into()));
        assert_eq!(pipeline.segments[0].env.get("EMPTY"), Some(&String::new()));
    }

    #[test]
    fn file_script_becomes_one_fail_fast_execution() {
        let spec = compile_file("echo one\necho two", "sample.cue").unwrap();
        assert!(matches!(spec.plan, ExecutionPlan::OnSuccess { .. }));
        assert_eq!(spec.source.unwrap().name, "sample.cue");
    }

    #[test]
    fn scope_commands_compile_to_context_deltas() {
        let spec = compiled(":run(scope=true) env set KEY=value -> cd /tmp -> env |> grep KEY");
        assert!(matches!(spec.plan, ExecutionPlan::OnSuccess { .. }));
        spec.plan.validate().unwrap();
    }

    #[test]
    fn script_rejects_mixed_launch_contexts() {
        let error = compile_file(
            ":run(pty=false) echo one\n:run(pty=true) echo two",
            "mixed.cue",
        )
        .unwrap_err();
        assert!(error.to_string().contains("one shared launch context"));
    }
}
