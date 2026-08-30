//! DSL-to-runtime compilation.
//!
//! The parser still exposes a rich frontend command enum internally, but this
//! module is the only public bridge from text to the daemon's typed execution
//! contract. `cued` never depends on this crate.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::Mode;
use cue_core::command::{ModeParams, ParamValue};
use cue_core::execution::{ExecutionPlan, ExecutionSpec, LaunchContext, SourceMetadata};
use cue_core::ipc::RequestPayload;
use cue_core::launch::{SandboxMode, SandboxSettings, SandboxUpper};
use cue_core::pipeline::{PipeSegment as CorePipeSegment, Pipeline as CorePipeline};
use cue_core::scope::EnvDelta;
use cue_core::{ExecutionId, ScheduleId, StepId};

use crate::ast::{ChainNode, JobExpr, ParallelOp, Pipeline as AstPipeline, SerialOp};
use crate::resolver::{ResolvedCommand, ResolvedForegroundRole};
use crate::{ParseError, parse_command, parse_file_script_command};

#[derive(Debug, Clone)]
pub enum CompiledCommand {
    Daemon(RequestPayload),
    Frontend(FrontendAction),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendAction {
    Help { topic: Option<String> },
    Clear,
    Quit,
    Restart,
    Retry { id: ExecutionId },
    Unsupported { message: String },
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
        CompiledCommand::Daemon(RequestPayload::SubmitExecution { spec }) => Ok(*spec),
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
            Ok(CompiledCommand::Daemon(RequestPayload::SubmitExecution {
                spec: Box::new(execution_spec(plan, launch_context, source_name)),
            }))
        }
        ResolvedCommand::Script { items, .. } => {
            if items.is_empty() {
                return Err(CompileError::Invalid("a .cue script is empty".into()));
            }
            let mut plans = Vec::with_capacity(items.len());
            let mut launch_context = None;
            for item in items {
                let source = item.source;
                let (plan, item_launch) = compile_script_item(*item.command).map_err(|error| {
                    CompileError::Invalid(format!("invalid script item `{source}`: {error}"))
                })?;
                if let Some(existing) = &launch_context {
                    if existing != &item_launch {
                        return Err(CompileError::Invalid(format!(
                            ".cue files require one shared launch context; `{source}` differs from earlier items, so split commands with different pty, resource, wrapper, or workspace-view settings into separate submissions"
                        )));
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
            Ok(CompiledCommand::Daemon(RequestPayload::SubmitExecution {
                spec: Box::new(execution_spec(
                    plan,
                    launch_context.unwrap_or_default(),
                    source_name,
                )),
            }))
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
            Ok(CompiledCommand::Daemon(RequestPayload::CreateSchedule {
                schedule,
                execution: Box::new(execution_spec(plan, launch_context, source_name)),
            }))
        }
        ResolvedCommand::Cd { path } => {
            Ok(CompiledCommand::Daemon(RequestPayload::ApplyScopeDelta {
                base: None,
                delta: EnvDelta {
                    set: BTreeMap::new(),
                    unset: Vec::new(),
                    cwd: Some(PathBuf::from(path)),
                },
            }))
        }
        ResolvedCommand::Umask { .. } => {
            Ok(CompiledCommand::Frontend(FrontendAction::Unsupported {
                message: "`:umask` requires the Cue vNext execution compiler".into(),
            }))
        }
        ResolvedCommand::Env { subcommand } => match compile_env_delta(subcommand.as_deref())? {
            Some(delta) => Ok(CompiledCommand::Daemon(RequestPayload::ApplyScopeDelta {
                base: None,
                delta,
            })),
            None => Ok(CompiledCommand::Daemon(RequestPayload::ShowEnv {
                tail_bytes: None,
            })),
        },
        ResolvedCommand::Jobs => Ok(CompiledCommand::Daemon(RequestPayload::ListExecutions {
            limit: None,
        })),
        ResolvedCommand::Crons => Ok(CompiledCommand::Daemon(RequestPayload::ListSchedules {
            limit: None,
        })),
        ResolvedCommand::Scopes | ResolvedCommand::Scope { subcommand: None } => {
            Ok(CompiledCommand::Daemon(RequestPayload::ListScopes {
                limit: None,
            }))
        }
        ResolvedCommand::Config { subcommand }
            if subcommand
                .as_deref()
                .map(str::trim)
                .is_none_or(|value| value.is_empty() || value == "show") =>
        {
            Ok(CompiledCommand::Daemon(RequestPayload::ShowConfig {
                tail_bytes: None,
            }))
        }
        ResolvedCommand::Kill { id } => {
            if let Ok(id) = id.parse::<ExecutionId>() {
                Ok(CompiledCommand::Daemon(RequestPayload::CancelExecution {
                    id,
                    mode: cue_core::execution::CancelMode::Force,
                }))
            } else if let Ok(id) = id.parse::<ScheduleId>() {
                Ok(CompiledCommand::Daemon(RequestPayload::RemoveSchedule {
                    id,
                }))
            } else {
                Err(CompileError::Invalid(format!(
                    "`:kill` expects an execution or schedule ID, got `{id}`"
                )))
            }
        }
        ResolvedCommand::Retry { id } => Ok(CompiledCommand::Frontend(FrontendAction::Retry {
            id: parse_execution_id("retry", &id)?,
        })),
        ResolvedCommand::Out { id, tail_bytes } => {
            let (id, step_id) = parse_execution_output_target("out", &id)?;
            Ok(CompiledCommand::Daemon(
                RequestPayload::ReadExecutionOutput {
                    id,
                    step_id,
                    stdout_bytes: tail_bytes,
                    stderr_bytes: Some(0),
                },
            ))
        }
        ResolvedCommand::Err { id } => {
            let (id, step_id) = parse_execution_output_target("err", &id)?;
            Ok(CompiledCommand::Daemon(
                RequestPayload::ReadExecutionOutput {
                    id,
                    step_id,
                    stdout_bytes: Some(0),
                    stderr_bytes: None,
                },
            ))
        }
        ResolvedCommand::Fg { id, role } => {
            let id = id.parse::<StepId>().map_err(|_| {
                CompileError::Invalid(format!(
                    "PTY attach expects a step ID such as E1/S1, got `{id}`"
                ))
            })?;
            Ok(CompiledCommand::Daemon(match role {
                ResolvedForegroundRole::Controller => RequestPayload::StepAttach { id },
                ResolvedForegroundRole::Observer => RequestPayload::StepWatch { id },
            }))
        }
        ResolvedCommand::Wait { id } => {
            Ok(CompiledCommand::Daemon(RequestPayload::WaitExecution {
                id: parse_execution_id("wait", &id)?,
            }))
        }
        ResolvedCommand::Cancel { id } => {
            Ok(CompiledCommand::Daemon(RequestPayload::CancelExecution {
                id: parse_execution_id("cancel", &id)?,
                mode: cue_core::execution::CancelMode::Graceful,
            }))
        }
        ResolvedCommand::Pause { id } => {
            Ok(CompiledCommand::Daemon(RequestPayload::PauseSchedule {
                id: parse_schedule_id("pause", &id)?,
            }))
        }
        ResolvedCommand::Resume { id } => {
            Ok(CompiledCommand::Daemon(RequestPayload::ResumeSchedule {
                id: parse_schedule_id("resume", &id)?,
            }))
        }
        ResolvedCommand::RemoveCron { id } => {
            Ok(CompiledCommand::Daemon(RequestPayload::RemoveSchedule {
                id: parse_schedule_id("remove", &id)?,
            }))
        }
        ResolvedCommand::Log { id: None } => {
            Ok(CompiledCommand::Daemon(RequestPayload::ListExecutions {
                limit: None,
            }))
        }
        ResolvedCommand::Log { id: Some(id) } => {
            Ok(CompiledCommand::Daemon(RequestPayload::GetExecution {
                id: parse_execution_id("log", &id)?,
            }))
        }
        ResolvedCommand::Providers | ResolvedCommand::Resources => {
            Ok(CompiledCommand::Daemon(RequestPayload::ListResources {}))
        }
        ResolvedCommand::Help { topic } => {
            Ok(CompiledCommand::Frontend(FrontendAction::Help { topic }))
        }
        ResolvedCommand::Clear => Ok(CompiledCommand::Frontend(FrontendAction::Clear)),
        ResolvedCommand::Quit => Ok(CompiledCommand::Frontend(FrontendAction::Quit)),
        ResolvedCommand::Restart => Ok(CompiledCommand::Frontend(FrontendAction::Restart)),
        command => Ok(CompiledCommand::Frontend(FrontendAction::Unsupported {
            message: format!(
                "command {command:?} has no IPC v3 frontend mapping; use E<n>, E<n>/S<n>, or T<n> typed targets"
            ),
        })),
    }
}

fn parse_execution_id(command: &str, input: &str) -> Result<ExecutionId, CompileError> {
    input.parse().map_err(|_| {
        CompileError::Invalid(format!(
            "`:{command}` expects an execution ID such as E1, got `{input}`"
        ))
    })
}

fn parse_schedule_id(command: &str, input: &str) -> Result<ScheduleId, CompileError> {
    input.parse().map_err(|_| {
        CompileError::Invalid(format!(
            "`:{command}` expects a schedule ID such as T1, got `{input}`"
        ))
    })
}

fn parse_execution_output_target(
    command: &str,
    input: &str,
) -> Result<(ExecutionId, Option<StepId>), CompileError> {
    if input.contains("/S") {
        let step = input.parse::<StepId>().map_err(|_| {
            CompileError::Invalid(format!(
                "`:{command}` expects an execution or step ID such as E1 or E1/S1, got `{input}`"
            ))
        })?;
        Ok((step.execution, Some(step)))
    } else {
        Ok((parse_execution_id(command, input)?, None))
    }
}

pub fn render_help(topic: Option<&str>) -> String {
    match topic.map(str::trim).filter(|topic| !topic.is_empty()) {
        Some("schedule" | "schedules" | "cron") => [
            "Cue schedules",
            "",
            "- `:schedule every 5m <command>` creates a durable typed template.",
            "- `:schedules` lists templates; `:pause T1`, `:resume T1`, and `:remove T1` control one.",
            "- Every trigger creates a fresh execution ID; ephemeral spawn adapters are forbidden.",
        ]
        .join("\n"),
        Some("execution" | "executions" | "job") => [
            "Cue executions",
            "",
            "- Bare input submits one typed execution (`E<n>`).",
            "- Process leaves have stable step IDs (`E<n>/S<n>`).",
            "- `:wait E1`, `:cancel E1`, `:kill E1`, `:retry E1` control an execution.",
            "- `:out E1`, `:tail E1/S1`, `:fg E1/S1`, `:watch E1/S1` inspect a step.",
            "- Prefix assignments such as `VAR=value script` affect only that process segment.",
        ]
        .join("\n"),
        Some(topic) => format!(
            "Unknown help topic `{topic}`. Available topics: execution, schedule."
        ),
        None => [
            "Cue unified execution runtime",
            "",
            "Bare input is compiled locally and submitted as a typed execution plan.",
            "Use `:help execution` or `:help schedule` for command details.",
            "Composition: `&&`, `||`, `->`, `~>`, `|||`, `|?|`; pipelines: `|>`, `|&>`, `|!>`.",
            "Scope: `:cd <dir>`, `:env set KEY=value`, `:env unset KEY`.",
        ]
        .join("\n"),
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
        ResolvedCommand::Umask { .. } => Err(CompileError::Invalid(
            "`:umask` requires the Cue vNext execution compiler".into(),
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
        ChainNode::Leaf(expression) => compile_job_expression(expression, scope_enabled),
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

fn compile_job_expression(
    expression: JobExpr,
    scope_enabled: bool,
) -> Result<ExecutionPlan, CompileError> {
    match expression {
        JobExpr::Pipeline(pipeline) => {
            let pipeline = compile_pipeline(pipeline);
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
        JobExpr::And { left, right } => Ok(ExecutionPlan::OnSuccess {
            left: Box::new(compile_job_expression(*left, scope_enabled)?),
            right: Box::new(compile_job_expression(*right, scope_enabled)?),
        }),
        JobExpr::Or { left, right } => Ok(ExecutionPlan::OnFailure {
            left: Box::new(compile_job_expression(*left, scope_enabled)?),
            right: Box::new(compile_job_expression(*right, scope_enabled)?),
        }),
    }
}

fn compile_pipeline(pipeline: AstPipeline) -> CorePipeline {
    CorePipeline {
        segments: pipeline
            .segments
            .into_iter()
            .map(|segment| CorePipeSegment {
                env: segment.env,
                command: segment.command,
                pipe_to_next: segment.pipe_to_next,
            })
            .collect(),
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
            CompiledCommand::Daemon(RequestPayload::SubmitExecution { spec }) => *spec,
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

    #[test]
    fn typed_commands_follow_the_shared_parser_and_compiler_path() {
        assert!(matches!(
            compile_command(":fg E7/S2", Mode::Job, "<test>").unwrap(),
            CompiledCommand::Daemon(RequestPayload::StepAttach {
                id: StepId {
                    execution: ExecutionId(7),
                    index: 2
                }
            })
        ));
        assert!(matches!(
            compile_command(":pause T3", Mode::Job, "<test>").unwrap(),
            CompiledCommand::Daemon(RequestPayload::PauseSchedule { id: ScheduleId(3) })
        ));
        assert!(matches!(
            compile_command(":retry E4", Mode::Job, "<test>").unwrap(),
            CompiledCommand::Frontend(FrontendAction::Retry { id: ExecutionId(4) })
        ));
    }
}
