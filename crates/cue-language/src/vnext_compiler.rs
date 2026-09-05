//! Cue surface syntax lowered into the closed vNext execution algebra.
//!
//! The caller supplies an explicit input scope. This compiler never reads the
//! process environment, keeps a session cursor, or asks the daemon to infer
//! execution semantics.

use std::collections::BTreeMap;

use cue_core::command::{ModeParams, ParamValue};
use cue_core::pipeline::{PipeOp, command_prefers_foreground};
use cue_core::vnext::{
    Argv, BuiltinCommand, EnvEdit, EnvKey, EnvPatch, ExecutionPlan, ExecutionSpec, FileModeMask,
    IoMode, PipeContinuation, PipeLink, Pipeline, Process, SequenceCondition,
};
use cue_core::{ExecutionId, ScopeHash, StepId};

use crate::ast::{ChainNode, JobExpr, ParallelOp, Pipeline as AstPipeline, SerialOp};
use crate::resolver::{ResolvedCommand, ResolvedForegroundRole};
use crate::{Mode, ParseError, parse_command, parse_file_script_command};

/// A language-owned intent. Client adapters translate these variants into the
/// v4 Query/Command envelopes and provide request and operation identities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VnextCommand {
    Submit(ExecutionSpec),
    ListExecutions,
    GetExecution {
        id: ExecutionId,
    },
    WaitExecution {
        id: ExecutionId,
    },
    ReadOutput {
        target: OutputTarget,
        stream: OutputSelection,
        tail_bytes: Option<usize>,
    },
    CancelExecution {
        id: ExecutionId,
        force: bool,
    },
    AttachPty {
        step: StepId,
        claim_control: bool,
    },
    Frontend(VnextFrontendAction),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputTarget {
    Execution(ExecutionId),
    Step(StepId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputSelection {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VnextFrontendAction {
    Help { topic: Option<String> },
    Clear,
    Quit,
    Restart,
}

#[derive(Debug, thiserror::Error)]
pub enum VnextCompileError {
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error("{0}")]
    Invalid(String),
    #[error("`{feature}` is not part of the Cue execution kernel; {owner}")]
    ExternalOwner {
        feature: &'static str,
        owner: &'static str,
    },
}

pub fn compile_vnext_command(
    input: &str,
    mode: Mode,
    scope: ScopeHash,
) -> Result<VnextCommand, VnextCompileError> {
    compile_resolved(parse_command(input, mode)?, scope)
}

pub fn compile_vnext_file(
    input: &str,
    scope: ScopeHash,
) -> Result<ExecutionSpec, VnextCompileError> {
    match compile_resolved(parse_file_script_command(input)?, scope)? {
        VnextCommand::Submit(spec) => Ok(spec),
        _ => Err(VnextCompileError::Invalid(
            "a .cue file must compile to one execution".into(),
        )),
    }
}

fn compile_resolved(
    command: ResolvedCommand,
    scope: ScopeHash,
) -> Result<VnextCommand, VnextCompileError> {
    match command {
        ResolvedCommand::Run { chain, params } => {
            let io = validate_run_params(&params)?;
            execution(scope, compile_chain(chain, io)?)
        }
        ResolvedCommand::Script { items } => {
            let mut plans = Vec::with_capacity(items.len());
            for item in items {
                let source = item.source;
                plans.push(compile_script_item(*item.command).map_err(|error| {
                    VnextCompileError::Invalid(format!("invalid script item `{source}`: {error}"))
                })?);
            }
            let plan = plans
                .into_iter()
                .reduce(|first, then| {
                    ExecutionPlan::sequence(first, then, SequenceCondition::Success)
                })
                .ok_or_else(|| VnextCompileError::Invalid("a .cue script is empty".into()))?;
            execution(scope, plan)
        }
        ResolvedCommand::Cd { path } => execution(scope, builtin_cd(&["cd".into(), path])?),
        ResolvedCommand::Env { subcommand } => {
            let subcommand = subcommand.ok_or_else(|| {
                VnextCompileError::Invalid(
                    "`:env` is not a kernel query; use `:env set KEY=VALUE` or `:env unset KEY`"
                        .into(),
                )
            })?;
            let mut words = vec!["env".to_owned()];
            words.extend(subcommand.split_whitespace().map(str::to_owned));
            execution(scope, builtin_env(&words)?)
        }
        ResolvedCommand::Umask { mask } => {
            execution(scope, builtin_umask(&["umask".into(), mask])?)
        }
        ResolvedCommand::Jobs | ResolvedCommand::Log { id: None } => {
            Ok(VnextCommand::ListExecutions)
        }
        ResolvedCommand::Log { id: Some(id) } => Ok(VnextCommand::GetExecution {
            id: parse_execution_id("log", &id)?,
        }),
        ResolvedCommand::Wait { id } => Ok(VnextCommand::WaitExecution {
            id: parse_execution_id("wait", &id)?,
        }),
        ResolvedCommand::Out { id, tail_bytes } => Ok(VnextCommand::ReadOutput {
            target: parse_output_target("out", &id)?,
            stream: OutputSelection::Stdout,
            tail_bytes,
        }),
        ResolvedCommand::Err { id } => Ok(VnextCommand::ReadOutput {
            target: parse_output_target("err", &id)?,
            stream: OutputSelection::Stderr,
            tail_bytes: None,
        }),
        ResolvedCommand::Cancel { id } => Ok(VnextCommand::CancelExecution {
            id: parse_execution_id("cancel", &id)?,
            force: false,
        }),
        ResolvedCommand::Kill { id } => Ok(VnextCommand::CancelExecution {
            id: parse_execution_id("kill", &id)?,
            force: true,
        }),
        ResolvedCommand::Fg { id, role } => Ok(VnextCommand::AttachPty {
            step: id.parse::<StepId>().map_err(|_| {
                VnextCompileError::Invalid(format!(
                    "PTY attach expects a step ID such as E1/S1, got `{id}`"
                ))
            })?,
            claim_control: matches!(role, ResolvedForegroundRole::Controller),
        }),
        ResolvedCommand::Help { topic } => {
            Ok(VnextCommand::Frontend(VnextFrontendAction::Help { topic }))
        }
        ResolvedCommand::Clear => Ok(VnextCommand::Frontend(VnextFrontendAction::Clear)),
        ResolvedCommand::Quit => Ok(VnextCommand::Frontend(VnextFrontendAction::Quit)),
        ResolvedCommand::Restart => Ok(VnextCommand::Frontend(VnextFrontendAction::Restart)),
        ResolvedCommand::Cron { .. }
        | ResolvedCommand::Crons
        | ResolvedCommand::Pause { .. }
        | ResolvedCommand::Resume { .. }
        | ResolvedCommand::RemoveCron { .. } => Err(VnextCompileError::ExternalOwner {
            feature: "schedule",
            owner: "a scheduler must submit ordinary ExecutionSpec values",
        }),
        ResolvedCommand::Retry { .. } => Err(VnextCompileError::ExternalOwner {
            feature: "retry",
            owner: "an orchestration layer must choose and submit the replacement execution",
        }),
        ResolvedCommand::Providers | ResolvedCommand::Resources => {
            Err(VnextCompileError::ExternalOwner {
                feature: "resource inspection",
                owner: "the composition host owns providers and resource policy",
            })
        }
        ResolvedCommand::Scopes | ResolvedCommand::Scope { .. } => {
            Err(VnextCompileError::ExternalOwner {
                feature: "scope history",
                owner: "clients may store explicit Scope values but the kernel has no scope cursor",
            })
        }
        ResolvedCommand::Config { .. } => Err(VnextCompileError::ExternalOwner {
            feature: "configuration",
            owner: "the hosting frontend owns configuration",
        }),
    }
}

fn execution(scope: ScopeHash, plan: ExecutionPlan) -> Result<VnextCommand, VnextCompileError> {
    Ok(VnextCommand::Submit(
        ExecutionSpec::new(scope, plan).map_err(invalid)?,
    ))
}

fn compile_script_item(command: ResolvedCommand) -> Result<ExecutionPlan, VnextCompileError> {
    match command {
        ResolvedCommand::Run { chain, params } => {
            let io = validate_run_params(&params)?;
            compile_chain(chain, io)
        }
        ResolvedCommand::Cd { path } => builtin_cd(&["cd".into(), path]),
        ResolvedCommand::Env { subcommand } => {
            let subcommand = subcommand.ok_or_else(|| {
                VnextCompileError::Invalid(
                    "`:env` without set/unset is not executable in a .cue file".into(),
                )
            })?;
            let mut words = vec!["env".to_owned()];
            words.extend(subcommand.split_whitespace().map(str::to_owned));
            builtin_env(&words)
        }
        ResolvedCommand::Umask { mask } => builtin_umask(&["umask".into(), mask]),
        _ => Err(VnextCompileError::Invalid(
            "a .cue file may contain runs and the `cd`, `env set|unset`, and `umask` builtins"
                .into(),
        )),
    }
}

fn validate_run_params(params: &ModeParams) -> Result<Option<IoMode>, VnextCompileError> {
    for name in params.params.keys() {
        if name != "pty" {
            return Err(VnextCompileError::ExternalOwner {
                feature: match name.as_str() {
                    "need" | "sandbox" | "sandbox.upper" => "resource or workspace policy",
                    "scope" => "session scope mutation",
                    "wrapper" => "spawn transformation",
                    "cwd" => "cwd mode parameter",
                    _ => "run mode parameter",
                },
                owner: match name.as_str() {
                    "cwd" => {
                        "write `cd PATH -> ...` so the scope transition is an explicit builtin step"
                    }
                    "wrapper" => {
                        "composition must resolve a typed spawn transform before execution"
                    }
                    "need" | "sandbox" | "sandbox.upper" => {
                        "composition must resolve workspace and resource providers before execution"
                    }
                    "scope" => "the vNext kernel threads Scope only inside an Execution",
                    _ => "the hosting frontend must resolve it before compilation",
                },
            });
        }
    }
    match params.get("pty") {
        None => Ok(None),
        Some(ParamValue::Bool(true)) => Ok(Some(IoMode::Pty)),
        Some(ParamValue::Bool(false)) => Ok(Some(IoMode::Captured)),
        Some(ParamValue::Str(_)) => Err(VnextCompileError::Invalid(
            "pty expects a boolean value".into(),
        )),
    }
}

fn compile_chain(
    node: ChainNode,
    io_override: Option<IoMode>,
) -> Result<ExecutionPlan, VnextCompileError> {
    match node {
        ChainNode::Leaf(expression) => compile_job_expression(expression, io_override),
        ChainNode::Serial { left, op, right } => Ok(ExecutionPlan::sequence(
            compile_chain(*left, io_override)?,
            compile_chain(*right, io_override)?,
            match op {
                SerialOp::Then => SequenceCondition::Success,
                SerialOp::Always => SequenceCondition::Always,
            },
        )),
        ChainNode::Parallel { left, op, right } => ExecutionPlan::parallel(
            vec![
                compile_chain(*left, io_override)?,
                compile_chain(*right, io_override)?,
            ],
            match op {
                ParallelOp::All => cue_core::vnext::ParallelJoin::All,
                ParallelOp::Race => cue_core::vnext::ParallelJoin::AnySuccess,
            },
        )
        .map_err(invalid),
    }
}

fn compile_job_expression(
    expression: JobExpr,
    io_override: Option<IoMode>,
) -> Result<ExecutionPlan, VnextCompileError> {
    match expression {
        JobExpr::Pipeline(pipeline) => compile_pipeline_or_builtin(pipeline, io_override),
        JobExpr::And { left, right } => Ok(ExecutionPlan::sequence(
            compile_job_expression(*left, io_override)?,
            compile_job_expression(*right, io_override)?,
            SequenceCondition::Success,
        )),
        JobExpr::Or { left, right } => Ok(ExecutionPlan::sequence(
            compile_job_expression(*left, io_override)?,
            compile_job_expression(*right, io_override)?,
            SequenceCondition::Failure,
        )),
    }
}

fn compile_pipeline_or_builtin(
    pipeline: AstPipeline,
    io_override: Option<IoMode>,
) -> Result<ExecutionPlan, VnextCompileError> {
    if pipeline.segments.len() == 1 {
        let segment = &pipeline.segments[0];
        if segment.env.is_empty() {
            if let Some(builtin) = compile_builtin(&segment.command)? {
                return Ok(builtin);
            }
        } else if builtin_name(&segment.command) {
            return Err(VnextCompileError::Invalid(
                "process-local assignments cannot prefix a Cue builtin".into(),
            ));
        }
    } else if pipeline
        .segments
        .iter()
        .any(|segment| builtin_name(&segment.command))
    {
        return Err(VnextCompileError::Invalid(
            "Cue builtins cannot be pipeline processes; compose them with `->`".into(),
        ));
    }

    let inferred_io = pipeline
        .segments
        .last()
        .is_some_and(|segment| command_prefers_foreground(&segment.command));
    let pipeline = compile_pipeline(pipeline)?;
    Ok(ExecutionPlan::run(
        pipeline,
        io_override.unwrap_or(if inferred_io {
            IoMode::Pty
        } else {
            IoMode::Captured
        }),
    ))
}

fn compile_pipeline(pipeline: AstPipeline) -> Result<Pipeline, VnextCompileError> {
    let mut segments = pipeline.segments.into_iter();
    let first = segments
        .next()
        .ok_or_else(|| VnextCompileError::Invalid("pipeline must contain a process".into()))?;
    let first_link = first.pipe_to_next;
    let first = compile_process(first.env, first.command)?;
    let mut previous_link = first_link;
    let mut rest = Vec::new();
    for segment in segments {
        let link = previous_link.ok_or_else(|| {
            VnextCompileError::Invalid("pipeline is missing a link between processes".into())
        })?;
        previous_link = segment.pipe_to_next;
        rest.push(PipeContinuation::new(
            compile_pipe_link(link),
            compile_process(segment.env, segment.command)?,
        ));
    }
    if previous_link.is_some() {
        return Err(VnextCompileError::Invalid(
            "pipeline has a dangling final link".into(),
        ));
    }
    Ok(Pipeline::new(first, rest))
}

fn compile_process(
    assignments: BTreeMap<String, String>,
    words: Vec<String>,
) -> Result<Process, VnextCompileError> {
    let argv = Argv::try_from(words).map_err(invalid)?;
    let edits = assignments
        .into_iter()
        .map(|(key, value)| {
            Ok((
                surface_env_key(&key)?,
                EnvEdit::set(value).map_err(invalid)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, VnextCompileError>>()?;
    Ok(if edits.is_empty() {
        Process::new(argv)
    } else {
        Process::with_env(argv, EnvPatch::new(edits))
    })
}

fn compile_pipe_link(link: PipeOp) -> PipeLink {
    match link {
        PipeOp::Stdout => PipeLink::StdoutToStdin,
        PipeOp::StdoutStderr => PipeLink::StdoutAndStderrToStdin,
        PipeOp::StderrOnly => PipeLink::StderrToStdin,
    }
}

fn compile_builtin(words: &[String]) -> Result<Option<ExecutionPlan>, VnextCompileError> {
    match words.first().map(String::as_str) {
        Some("cd") => builtin_cd(words).map(Some),
        Some("env")
            if words
                .get(1)
                .is_some_and(|word| word == "set" || word == "unset") =>
        {
            builtin_env(words).map(Some)
        }
        Some("umask") => builtin_umask(words).map(Some),
        _ => Ok(None),
    }
}

fn builtin_name(words: &[String]) -> bool {
    matches!(words.first().map(String::as_str), Some("cd" | "umask"))
        || matches!(
            words.get(0..2),
            Some([command, subcommand])
                if command == "env" && (subcommand == "set" || subcommand == "unset")
        )
}

fn builtin_cd(words: &[String]) -> Result<ExecutionPlan, VnextCompileError> {
    let [command, path] = words else {
        return Err(VnextCompileError::Invalid(
            "`cd` expects exactly one path".into(),
        ));
    };
    debug_assert_eq!(command, "cd");
    Ok(ExecutionPlan::builtin(
        BuiltinCommand::cd(path).map_err(invalid)?,
    ))
}

fn builtin_env(words: &[String]) -> Result<ExecutionPlan, VnextCompileError> {
    let [command, subcommand, arguments @ ..] = words else {
        return Err(VnextCompileError::Invalid(
            "`env` expects `set KEY=VALUE ...` or `unset KEY ...`".into(),
        ));
    };
    debug_assert_eq!(command, "env");
    if arguments.is_empty() {
        return Err(VnextCompileError::Invalid(format!(
            "`env {subcommand}` requires at least one argument"
        )));
    }
    let edits = match subcommand.as_str() {
        "set" => arguments
            .iter()
            .map(|assignment| {
                let (key, value) = assignment.split_once('=').ok_or_else(|| {
                    VnextCompileError::Invalid(format!(
                        "`env set` expects KEY=VALUE, got `{assignment}`"
                    ))
                })?;
                Ok((surface_env_key(key)?, EnvEdit::set(value).map_err(invalid)?))
            })
            .collect::<Result<BTreeMap<_, _>, VnextCompileError>>()?,
        "unset" => arguments
            .iter()
            .map(|key| Ok((surface_env_key(key)?, EnvEdit::Unset)))
            .collect::<Result<BTreeMap<_, _>, VnextCompileError>>()?,
        _ => {
            return Err(VnextCompileError::Invalid(
                "`env` expects `set KEY=VALUE ...` or `unset KEY ...`".into(),
            ));
        }
    };
    Ok(ExecutionPlan::builtin(
        BuiltinCommand::env(EnvPatch::new(edits)).map_err(invalid)?,
    ))
}

fn builtin_umask(words: &[String]) -> Result<ExecutionPlan, VnextCompileError> {
    let [command, mask] = words else {
        return Err(VnextCompileError::Invalid(
            "`umask` expects exactly one octal mask".into(),
        ));
    };
    debug_assert_eq!(command, "umask");
    let mask = u16::from_str_radix(mask.trim_start_matches("0o"), 8)
        .map_err(|_| VnextCompileError::Invalid(format!("invalid octal umask `{mask}`")))?;
    Ok(ExecutionPlan::builtin(BuiltinCommand::umask(
        FileModeMask::new(mask).map_err(invalid)?,
    )))
}

fn surface_env_key(key: &str) -> Result<EnvKey, VnextCompileError> {
    let mut characters = key.chars();
    if !characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err(VnextCompileError::Invalid(format!(
            "invalid surface environment variable name `{key}`"
        )));
    }
    EnvKey::new(key).map_err(invalid)
}

fn parse_execution_id(command: &str, input: &str) -> Result<ExecutionId, VnextCompileError> {
    input.parse::<ExecutionId>().map_err(|_| {
        VnextCompileError::Invalid(format!(
            "`:{command}` expects an execution ID such as E1, got `{input}`"
        ))
    })
}

fn parse_output_target(command: &str, input: &str) -> Result<OutputTarget, VnextCompileError> {
    if input.contains('/') {
        input
            .parse::<StepId>()
            .map(OutputTarget::Step)
            .map_err(|_| {
                VnextCompileError::Invalid(format!(
                    "`:{command}` expects E1 or E1/S1, got `{input}`"
                ))
            })
    } else {
        parse_execution_id(command, input).map(OutputTarget::Execution)
    }
}

fn invalid(error: impl std::fmt::Display) -> VnextCompileError {
    VnextCompileError::Invalid(error.to_string())
}

#[cfg(test)]
mod tests {
    use cue_core::vnext::{BuiltinCommand, EnvEdit, ExecutionPlan, IoMode, SequenceCondition};

    use super::*;

    const SCOPE: ScopeHash = ScopeHash([7; 32]);

    fn plan(input: &str) -> ExecutionPlan {
        match compile_vnext_command(input, Mode::Job, SCOPE).unwrap() {
            VnextCommand::Submit(spec) => spec.plan().clone(),
            command => panic!("expected submission, got {command:?}"),
        }
    }

    #[test]
    fn core_builtin_surface_has_exactly_cd_env_and_umask() {
        let commands = ["cd repo", "env set MODE=release", "umask 027"];
        let builtins = commands.map(plan);
        assert!(matches!(
            builtins[0],
            ExecutionPlan::Builtin {
                command: BuiltinCommand::Cd(_)
            }
        ));
        assert!(matches!(
            builtins[1],
            ExecutionPlan::Builtin {
                command: BuiltinCommand::Env(_)
            }
        ));
        assert!(matches!(
            builtins[2],
            ExecutionPlan::Builtin {
                command: BuiltinCommand::Umask(_)
            }
        ));
    }

    #[test]
    fn sequence_threads_builtin_scope_and_preserves_conditions() {
        let plan = plan("cd repo -> env set MODE=release -> cargo test || recover");
        let ExecutionPlan::Sequence {
            first,
            then,
            when: SequenceCondition::Success,
        } = plan
        else {
            panic!("expected outer sequence");
        };
        assert!(matches!(*first, ExecutionPlan::Sequence { .. }));
        assert!(matches!(
            *then,
            ExecutionPlan::Sequence {
                when: SequenceCondition::Failure,
                ..
            }
        ));
    }

    #[test]
    fn prefix_assignment_is_process_local_and_argument_assignment_is_literal() {
        let ExecutionPlan::Run { pipeline, .. } = plan("A=one printenv A |> env A=two") else {
            panic!("expected run");
        };
        let processes = pipeline.processes().collect::<Vec<_>>();
        let key = EnvKey::new("A").unwrap();
        assert!(matches!(
            processes[0].env().get(&key),
            Some(EnvEdit::Set(_))
        ));
        assert!(processes[1].env().is_empty());
        assert_eq!(processes[1].argv().words(), ["env", "A=two"]);
    }

    #[test]
    fn surface_environment_names_remain_shell_like() {
        let error = compile_vnext_command(":env set A-B=value", Mode::Job, SCOPE).unwrap_err();
        assert!(error.to_string().contains("invalid surface environment"));
    }

    #[test]
    fn builtins_cannot_be_disguised_as_pipeline_processes() {
        let error = compile_vnext_command("cd repo |> pwd", Mode::Job, SCOPE).unwrap_err();
        assert!(error.to_string().contains("cannot be pipeline processes"));

        let error = compile_vnext_command("A=one cd repo", Mode::Job, SCOPE).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("assignments cannot prefix a Cue builtin")
        );
    }

    #[test]
    fn pty_is_resolved_per_run_and_never_attached_to_builtin() {
        assert!(matches!(
            plan("vim file"),
            ExecutionPlan::Run {
                io: IoMode::Pty,
                ..
            }
        ));
        assert!(matches!(
            plan(":run(pty=false) vim file"),
            ExecutionPlan::Run {
                io: IoMode::Captured,
                ..
            }
        ));
        assert!(matches!(plan("cd repo"), ExecutionPlan::Builtin { .. }));
    }

    #[test]
    fn files_can_mix_captured_and_pty_runs() {
        let spec =
            compile_vnext_file("echo prepare\n:run(pty=true) vim result.txt", SCOPE).unwrap();
        let ExecutionPlan::Sequence { first, then, .. } = spec.plan() else {
            panic!("expected sequence");
        };
        assert!(matches!(
            first.as_ref(),
            ExecutionPlan::Run {
                io: IoMode::Captured,
                ..
            }
        ));
        assert!(matches!(
            then.as_ref(),
            ExecutionPlan::Run {
                io: IoMode::Pty,
                ..
            }
        ));
    }

    #[test]
    fn schedules_retry_resources_and_scope_history_have_external_owners() {
        for input in [
            ":schedule every 5m echo ok",
            ":retry E1",
            ":resources",
            ":scopes",
        ] {
            assert!(matches!(
                compile_vnext_command(input, Mode::Job, SCOPE),
                Err(VnextCompileError::ExternalOwner { .. })
            ));
        }
    }

    #[test]
    fn legacy_launch_parameters_do_not_leak_into_core() {
        for input in [
            ":run(cwd=/tmp) pwd",
            ":run(wrapper=true) echo ok",
            ":run(sandbox=overlay) echo ok",
            ":run(scope=true) cd repo",
        ] {
            assert!(matches!(
                compile_vnext_command(input, Mode::Job, SCOPE),
                Err(VnextCompileError::ExternalOwner { .. })
            ));
        }
    }
}
