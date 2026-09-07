use cue_core::StepId;
use cue_core::vnext::{AbsolutePath, BuiltinCommand, BuiltinSuccess, Scope};

use crate::{RuntimeError, RuntimeErrorKind};

/// Realize a committed builtin without changing ambient process state or
/// acquiring resources. Its typed result must still be committed by Core.
pub fn realize_builtin(
    step: StepId,
    command: &BuiltinCommand,
    scope: &Scope,
) -> Result<BuiltinSuccess, RuntimeError> {
    match command {
        BuiltinCommand::Env(_) => Ok(BuiltinSuccess::Env),
        BuiltinCommand::Umask(_) => Ok(BuiltinSuccess::Umask),
        BuiltinCommand::Cd(path) => {
            let path = scope.cwd().as_path().join(path.as_path());
            let resolved = std::fs::canonicalize(&path).map_err(|error| {
                RuntimeError::new(
                    RuntimeErrorKind::InvalidInput,
                    format!("resolve directory for {step}: {error}"),
                )
            })?;
            if !resolved.is_dir() {
                return Err(RuntimeError::new(
                    RuntimeErrorKind::InvalidInput,
                    format!("cd target for {step} is not a directory"),
                ));
            }
            let cwd = AbsolutePath::new(resolved).map_err(|error| {
                RuntimeError::new(RuntimeErrorKind::InvalidInput, error.to_string())
            })?;
            Ok(BuiltinSuccess::Cd { cwd })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cue_core::ExecutionId;
    use cue_core::vnext::{EnvEdit, EnvKey, EnvPatch, FileModeMask};
    use std::collections::BTreeMap;

    #[test]
    fn builtin_replay_has_only_a_typed_result_and_keeps_inputs_unchanged() {
        let cwd = std::env::current_dir().unwrap();
        let scope = Scope::new(
            AbsolutePath::new(&cwd).unwrap(),
            BTreeMap::new(),
            FileModeMask::new(0o077).unwrap(),
        );
        let before = scope.clone();
        let step = StepId {
            execution: ExecutionId(1),
            index: 1,
        };
        let commands = [
            BuiltinCommand::cd(".").unwrap(),
            BuiltinCommand::env(EnvPatch::new(BTreeMap::from([(
                EnvKey::new("CUE_BUILTIN_TEST").unwrap(),
                EnvEdit::set("value").unwrap(),
            )])))
            .unwrap(),
            BuiltinCommand::umask(FileModeMask::new(0o022).unwrap()),
        ];
        for command in commands {
            assert_eq!(
                realize_builtin(step, &command, &scope).unwrap(),
                realize_builtin(step, &command, &scope).unwrap()
            );
        }
        assert_eq!(scope, before);
        assert_eq!(std::env::current_dir().unwrap(), cwd);
        assert!(std::env::var_os("CUE_BUILTIN_TEST").is_none());
    }
}
