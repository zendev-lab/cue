//! Metadata for frontend `:` commands and their language syntax.

/// High-level command grouping used by help and documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandCategory {
    Execution,
    Schedule,
    Scope,
    System,
}

/// Parser-facing argument classification for a `:` command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandArgKind {
    Chain,
    Schedule,
    Id(CommandIdKind),
    Tail(CommandIdKind),
    OptionalId(CommandIdKind),
    OptionalText,
    Empty,
}

/// Entity ID shape accepted by a command argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandIdKind {
    Execution,
    Step,
    ExecutionOrStep,
    Schedule,
    ExecutionOrSchedule,
}

impl CommandIdKind {
    pub fn accepts_execution(self) -> bool {
        matches!(
            self,
            Self::Execution | Self::ExecutionOrStep | Self::ExecutionOrSchedule
        )
    }

    pub fn accepts_step(self) -> bool {
        matches!(self, Self::Step | Self::ExecutionOrStep)
    }

    pub fn accepts_schedule(self) -> bool {
        matches!(self, Self::Schedule | Self::ExecutionOrSchedule)
    }

    pub fn display(self) -> &'static str {
        match self {
            Self::Execution => "E<n>",
            Self::Step => "E<n>/S<n>",
            Self::ExecutionOrStep => "E<n> or E<n>/S<n>",
            Self::Schedule => "T<n>",
            Self::ExecutionOrSchedule => "E<n>, E<n>/S<n>, or T<n>",
        }
    }

    pub fn first_example(self) -> &'static str {
        match self {
            Self::Execution | Self::ExecutionOrStep | Self::ExecutionOrSchedule => "E1",
            Self::Step => "E1/S1",
            Self::Schedule => "T1",
        }
    }
}

/// Static command metadata shared by parser, help, and completions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandSpec {
    pub name: &'static str,
    pub category: CommandCategory,
    pub arg_kind: CommandArgKind,
    pub usage: &'static str,
    pub detail: &'static str,
    pub documented: bool,
}

impl CommandSpec {
    pub fn visible_in_category(&self, category: CommandCategory) -> bool {
        if self.category == category {
            return true;
        }

        matches!(
            (self.arg_kind, category),
            (
                CommandArgKind::Id(CommandIdKind::ExecutionOrSchedule)
                    | CommandArgKind::OptionalId(CommandIdKind::ExecutionOrSchedule),
                CommandCategory::Execution | CommandCategory::Schedule
            )
        )
    }

    pub fn accepts_mode_params(&self) -> bool {
        mode_param_specs_for_command(self.name).next().is_some()
    }
}

/// Static mode-parameter metadata for completion and docs checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModeParamSpec {
    pub name: &'static str,
    pub commands: &'static [&'static str],
    pub value_kind: ModeParamValueKind,
    pub value_hint: &'static str,
    pub detail: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModeParamValueKind {
    String,
    Bool,
}

const MODE_PARAM_SPECS: &[ModeParamSpec] = &[
    ModeParamSpec {
        name: "cwd",
        commands: &["run", "schedule", "cron"],
        value_kind: ModeParamValueKind::String,
        value_hint: "/path",
        detail: "Run from this working directory without changing the session cwd",
    },
    ModeParamSpec {
        name: "wrapper",
        commands: &["run", "schedule", "cron"],
        value_kind: ModeParamValueKind::Bool,
        value_hint: "true",
        detail: "Override the runtime wrapper for this invocation",
    },
    ModeParamSpec {
        name: "scope",
        commands: &["run", "schedule", "cron"],
        value_kind: ModeParamValueKind::Bool,
        value_hint: "true",
        detail: "Allow run jobs to update the chain scope",
    },
    ModeParamSpec {
        name: "pty",
        commands: &["run"],
        value_kind: ModeParamValueKind::Bool,
        value_hint: "false",
        detail: "Run the job without allocating a PTY",
    },
    ModeParamSpec {
        name: "sandbox",
        commands: &["run"],
        value_kind: ModeParamValueKind::String,
        value_hint: "overlay",
        detail: "Run the job inside an opt-in sandbox",
    },
    ModeParamSpec {
        name: "sandbox.upper",
        commands: &["run"],
        value_kind: ModeParamValueKind::String,
        value_hint: "tmpfs",
        detail: "Use tmpfs or a directory path for the overlay sandbox upperdir",
    },
    ModeParamSpec {
        name: "need.<resource>",
        commands: &["run"],
        value_kind: ModeParamValueKind::String,
        value_hint: "1",
        detail: "Declare a provider-owned resource need quantity",
    },
];

impl ModeParamSpec {
    pub fn applies_to(&self, command: &str) -> bool {
        self.commands.contains(&command)
    }
}

pub const COMMAND_SPECS: &[CommandSpec] = &[
    CommandSpec {
        name: "run",
        category: CommandCategory::Execution,
        arg_kind: CommandArgKind::Chain,
        usage: ":run <command>",
        detail: "Compile and submit one typed execution",
        documented: true,
    },
    CommandSpec {
        name: "schedule",
        category: CommandCategory::Schedule,
        arg_kind: CommandArgKind::Schedule,
        usage: ":schedule <schedule> <command>",
        detail: "Create a typed schedule template",
        documented: true,
    },
    CommandSpec {
        name: "cron",
        category: CommandCategory::Schedule,
        arg_kind: CommandArgKind::Schedule,
        usage: ":cron <schedule> <command>",
        detail: "Internal parser spelling for :schedule",
        documented: false,
    },
    CommandSpec {
        name: "kill",
        category: CommandCategory::Execution,
        arg_kind: CommandArgKind::Id(CommandIdKind::ExecutionOrSchedule),
        usage: ":kill E<n> | :kill T<n>",
        detail: "Force-cancel an execution or remove a schedule",
        documented: true,
    },
    CommandSpec {
        name: "retry",
        category: CommandCategory::Execution,
        arg_kind: CommandArgKind::Id(CommandIdKind::Execution),
        usage: ":retry E<n>",
        detail: "Submit a new execution from an existing spec",
        documented: true,
    },
    CommandSpec {
        name: "out",
        category: CommandCategory::Execution,
        arg_kind: CommandArgKind::Id(CommandIdKind::ExecutionOrStep),
        usage: ":out E<n>[/S<n>]",
        detail: "Read captured stdout for an execution or step",
        documented: true,
    },
    CommandSpec {
        name: "tail",
        category: CommandCategory::Execution,
        arg_kind: CommandArgKind::Tail(CommandIdKind::ExecutionOrStep),
        usage: ":tail E<n>[/S<n>] [bytes]",
        detail: "Read a bounded stdout tail",
        documented: true,
    },
    CommandSpec {
        name: "err",
        category: CommandCategory::Execution,
        arg_kind: CommandArgKind::Id(CommandIdKind::ExecutionOrStep),
        usage: ":err E<n>[/S<n>]",
        detail: "Read captured stderr or merged PTY output",
        documented: true,
    },
    CommandSpec {
        name: "fg",
        category: CommandCategory::Execution,
        arg_kind: CommandArgKind::Id(CommandIdKind::Step),
        usage: ":fg E<n>/S<n>",
        detail: "Attach to a PTY step and claim control",
        documented: true,
    },
    CommandSpec {
        name: "watch",
        category: CommandCategory::Execution,
        arg_kind: CommandArgKind::Id(CommandIdKind::Step),
        usage: ":watch E<n>/S<n>",
        detail: "Observe a PTY step without taking control",
        documented: true,
    },
    CommandSpec {
        name: "wait",
        category: CommandCategory::Execution,
        arg_kind: CommandArgKind::Id(CommandIdKind::Execution),
        usage: ":wait E<n>",
        detail: "Wait for an execution to reach a terminal state",
        documented: true,
    },
    CommandSpec {
        name: "cancel",
        category: CommandCategory::Execution,
        arg_kind: CommandArgKind::Id(CommandIdKind::Execution),
        usage: ":cancel E<n>",
        detail: "Gracefully cancel an execution",
        documented: true,
    },
    CommandSpec {
        name: "pause",
        category: CommandCategory::Schedule,
        arg_kind: CommandArgKind::Id(CommandIdKind::Schedule),
        usage: ":pause T<n>",
        detail: "Pause a schedule",
        documented: true,
    },
    CommandSpec {
        name: "resume",
        category: CommandCategory::Schedule,
        arg_kind: CommandArgKind::Id(CommandIdKind::Schedule),
        usage: ":resume T<n>",
        detail: "Resume a schedule",
        documented: true,
    },
    CommandSpec {
        name: "remove",
        category: CommandCategory::Schedule,
        arg_kind: CommandArgKind::Id(CommandIdKind::Schedule),
        usage: ":remove T<n>",
        detail: "Remove a schedule",
        documented: true,
    },
    CommandSpec {
        name: "log",
        category: CommandCategory::Execution,
        arg_kind: CommandArgKind::OptionalId(CommandIdKind::Execution),
        usage: ":log [E<n>]",
        detail: "List executions or inspect one execution",
        documented: true,
    },
    CommandSpec {
        name: "executions",
        category: CommandCategory::Execution,
        arg_kind: CommandArgKind::Empty,
        usage: ":executions",
        detail: "List typed executions",
        documented: true,
    },
    CommandSpec {
        name: "schedules",
        category: CommandCategory::Schedule,
        arg_kind: CommandArgKind::Empty,
        usage: ":schedules",
        detail: "List typed schedule templates",
        documented: true,
    },
    CommandSpec {
        name: "scopes",
        category: CommandCategory::Scope,
        arg_kind: CommandArgKind::Empty,
        usage: ":scopes",
        detail: "List known scope snapshots",
        documented: true,
    },
    CommandSpec {
        name: "providers",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::Empty,
        usage: ":providers",
        detail: "List registered resource providers and their keys",
        documented: true,
    },
    CommandSpec {
        name: "resources",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::Empty,
        usage: ":resources",
        detail: "Show resource provider capacity snapshots",
        documented: true,
    },
    CommandSpec {
        name: "env",
        category: CommandCategory::Scope,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":env [subcommand]",
        detail: "Inspect or update the current session environment",
        documented: true,
    },
    CommandSpec {
        name: "cd",
        category: CommandCategory::Scope,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":cd <path>",
        detail: "Move the current session working directory",
        documented: true,
    },
    CommandSpec {
        name: "scope",
        category: CommandCategory::Scope,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":scope list",
        detail: "Inspect scope snapshots",
        documented: true,
    },
    CommandSpec {
        name: "help",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":help [topic]",
        detail: "Show command and mode help",
        documented: true,
    },
    CommandSpec {
        name: "config",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":config [subcommand]",
        detail: "Inspect runtime configuration",
        documented: true,
    },
    CommandSpec {
        name: "restart",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::Empty,
        usage: ":restart",
        detail: "Restart the local daemon through cue-client lifecycle",
        documented: true,
    },
    CommandSpec {
        name: "clear",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::Empty,
        usage: ":clear",
        detail: "Clear the frontend input/output view",
        documented: true,
    },
    CommandSpec {
        name: "quit",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::Empty,
        usage: ":quit",
        detail: "Quit the frontend",
        documented: true,
    },
    CommandSpec {
        name: "exit",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::Empty,
        usage: ":exit",
        detail: "Alias for :quit",
        documented: true,
    },
];

pub fn command_spec(name: &str) -> Option<&'static CommandSpec> {
    COMMAND_SPECS.iter().find(|spec| spec.name == name)
}

pub fn mode_param_spec(name: &str) -> Option<&'static ModeParamSpec> {
    MODE_PARAM_SPECS.iter().find(|spec| spec.name == name)
}

pub fn mode_param_spec_for_command(command: &str, name: &str) -> Option<&'static ModeParamSpec> {
    MODE_PARAM_SPECS
        .iter()
        .find(|spec| spec.name == name && spec.applies_to(command))
}

pub fn mode_param_specs_for_command(
    command: &str,
) -> impl Iterator<Item = &'static ModeParamSpec> + '_ {
    MODE_PARAM_SPECS
        .iter()
        .filter(move |spec| spec.applies_to(command))
}

pub fn command_names() -> impl Iterator<Item = &'static str> {
    COMMAND_SPECS
        .iter()
        .filter(|spec| spec.documented)
        .map(|spec| spec.name)
}

pub fn command_suggestions(name: &str) -> Vec<&'static str> {
    COMMAND_SPECS
        .iter()
        .filter(|spec| spec.documented)
        .filter(|spec| {
            spec.name.starts_with(&name[..1.min(name.len())]) || edit_distance(name, spec.name) <= 2
        })
        .map(|spec| spec.name)
        .collect()
}

fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut dp = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for (i, row) in dp.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in dp[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            dp[i][j] = (dp[i - 1][j] + 1)
                .min(dp[i][j - 1] + 1)
                .min(dp[i - 1][j - 1] + cost);
        }
    }
    dp[a.len()][b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_names_are_unique() {
        let mut names = std::collections::BTreeSet::new();
        for spec in COMMAND_SPECS {
            assert!(names.insert(spec.name), "duplicate command `{}`", spec.name);
        }
    }

    #[test]
    fn mode_param_names_are_unique() {
        let mut names = std::collections::BTreeSet::new();
        for spec in MODE_PARAM_SPECS {
            assert!(
                names.insert(spec.name),
                "duplicate mode parameter `{}`",
                spec.name
            );
        }
    }

    #[test]
    fn mode_param_command_boundaries_are_explicit() {
        assert!(mode_param_spec_for_command("run", "pty").is_some());
        assert!(mode_param_spec_for_command("run", "sandbox").is_some());
        assert!(mode_param_spec_for_command("run", "sandbox.upper").is_some());
        assert!(mode_param_spec_for_command("cron", "pty").is_none());
        assert!(mode_param_spec_for_command("cron", "sandbox").is_none());
        assert!(mode_param_spec_for_command("cron", "cwd").is_some());
        assert!(mode_param_spec_for_command("run", "need.<resource>").is_some());
        assert!(mode_param_spec_for_command("cron", "need.<resource>").is_none());
        assert!(command_spec("run").is_some_and(CommandSpec::accepts_mode_params));
        assert!(command_spec("cron").is_some_and(CommandSpec::accepts_mode_params));
        assert!(!command_spec("kill").is_some_and(CommandSpec::accepts_mode_params));
    }

    #[test]
    fn id_command_boundaries_are_explicit() {
        assert_eq!(
            command_spec("fg").map(|spec| spec.arg_kind),
            Some(CommandArgKind::Id(CommandIdKind::Step))
        );
        assert_eq!(
            command_spec("watch").map(|spec| spec.arg_kind),
            Some(CommandArgKind::Id(CommandIdKind::Step))
        );
        assert_eq!(
            command_spec("pause").map(|spec| spec.arg_kind),
            Some(CommandArgKind::Id(CommandIdKind::Schedule))
        );
        assert_eq!(
            command_spec("kill").map(|spec| spec.arg_kind),
            Some(CommandArgKind::Id(CommandIdKind::ExecutionOrSchedule))
        );
        assert_eq!(
            command_spec("log").map(|spec| spec.arg_kind),
            Some(CommandArgKind::OptionalId(CommandIdKind::Execution))
        );
    }

    #[test]
    fn cross_entity_commands_are_visible_in_each_help_category() {
        let kill = command_spec("kill").expect("kill command spec");
        assert!(kill.visible_in_category(CommandCategory::Execution));
        assert!(kill.visible_in_category(CommandCategory::Schedule));

        let log = command_spec("log").expect("log command spec");
        assert!(log.visible_in_category(CommandCategory::Execution));
        assert!(!log.visible_in_category(CommandCategory::Schedule));

        let pause = command_spec("pause").expect("pause command spec");
        assert!(!pause.visible_in_category(CommandCategory::Execution));
        assert!(pause.visible_in_category(CommandCategory::Schedule));
    }

    #[test]
    fn suggestions_include_close_matches() {
        assert!(command_suggestions("rn").contains(&"run"));
        assert!(command_suggestions("schedule").contains(&"schedule"));
        assert!(!command_suggestions("crn").contains(&"cron"));
    }

    #[test]
    fn documented_commands_exclude_legacy_spellings() {
        let names = command_names().collect::<Vec<_>>();
        for legacy in ["cron", "jobs", "crons", "send", "wrap", "pty"] {
            assert!(!names.contains(&legacy), "legacy command leaked: {legacy}");
        }
        assert!(names.contains(&"executions"));
        assert!(names.contains(&"schedules"));
    }
}
