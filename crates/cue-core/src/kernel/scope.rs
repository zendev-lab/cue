//! Content-addressed execution scope snapshots.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::ScopeHash;

use super::{Env, EnvPatch};

const SCOPE_HASH_DOMAIN: &[u8] = b"cue-scope-v2\0";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct AbsolutePath(PathBuf);

impl AbsolutePath {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, ScopeError> {
        let path = path.into();
        if path.as_os_str().is_empty() {
            return Err(ScopeError::EmptyPath);
        }
        if path.as_os_str().as_encoded_bytes().contains(&0) {
            return Err(ScopeError::PathContainsNul);
        }
        if !path.is_absolute() {
            return Err(ScopeError::PathNotAbsolute(path));
        }
        Ok(Self(path))
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }

    pub fn into_path_buf(self) -> PathBuf {
        self.0
    }
}

impl<'de> Deserialize<'de> for AbsolutePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let path = PathBuf::deserialize(deserializer)?;
        Self::new(path).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct FileModeMask(u16);

impl FileModeMask {
    pub fn new(mask: u16) -> Result<Self, ScopeError> {
        if mask & !0o777 != 0 {
            return Err(ScopeError::InvalidUmask(mask));
        }
        Ok(Self(mask))
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

impl<'de> Deserialize<'de> for FileModeMask {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mask = u16::deserialize(deserializer)?;
        Self::new(mask).map_err(serde::de::Error::custom)
    }
}

/// Complete, immutable execution scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    cwd: AbsolutePath,
    env: Env,
    umask: FileModeMask,
}

impl Scope {
    pub fn new(cwd: AbsolutePath, env: Env, umask: FileModeMask) -> Self {
        Self { cwd, env, umask }
    }

    pub fn cwd(&self) -> &AbsolutePath {
        &self.cwd
    }

    pub fn env(&self) -> &Env {
        &self.env
    }

    pub const fn umask(&self) -> FileModeMask {
        self.umask
    }

    pub fn with_cwd(&self, cwd: AbsolutePath) -> Self {
        Self {
            cwd,
            env: self.env.clone(),
            umask: self.umask,
        }
    }

    pub fn with_umask(&self, umask: FileModeMask) -> Self {
        Self {
            cwd: self.cwd.clone(),
            env: self.env.clone(),
            umask,
        }
    }

    pub fn apply_env(&self, patch: &EnvPatch) -> Self {
        Self {
            cwd: self.cwd.clone(),
            env: patch.apply(&self.env),
            umask: self.umask,
        }
    }

    pub fn compute_hash(&self) -> ScopeHash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(SCOPE_HASH_DOMAIN);
        update_bytes(
            &mut hasher,
            self.cwd.as_path().as_os_str().as_encoded_bytes(),
        );
        hasher.update(&self.umask.get().to_le_bytes());
        hasher.update(&(self.env.len() as u64).to_le_bytes());
        for (key, value) in &self.env {
            update_bytes(&mut hasher, key.as_str().as_bytes());
            update_bytes(&mut hasher, value.as_str().as_bytes());
            hasher.update(&[match value.sensitivity() {
                super::Sensitivity::Normal => 0,
                super::Sensitivity::Sensitive => 1,
            }]);
        }
        ScopeHash(*hasher.finalize().as_bytes())
    }
}

fn update_bytes(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ScopeError {
    #[error("scope path must not be empty")]
    EmptyPath,
    #[error("scope path contains NUL")]
    PathContainsNul,
    #[error("scope path must be absolute: {0:?}")]
    PathNotAbsolute(PathBuf),
    #[error("umask must contain only permission bits, got {0:#o}")]
    InvalidUmask(u16),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{EnvEdit, EnvKey, EnvValue};

    fn key(value: &str) -> EnvKey {
        EnvKey::new(value).unwrap()
    }

    fn value(value: &str) -> EnvValue {
        EnvValue::new(value).unwrap()
    }

    fn scope() -> Scope {
        Scope::new(
            AbsolutePath::new("/workspace").unwrap(),
            BTreeMap::from([(key("HOME"), value("/home/user"))]),
            FileModeMask::new(0o022).unwrap(),
        )
    }

    #[test]
    fn scope_hash_covers_cwd_env_and_umask() {
        let base = scope();
        assert_ne!(
            base.compute_hash(),
            base.with_cwd(AbsolutePath::new("/other").unwrap())
                .compute_hash()
        );
        assert_ne!(
            base.compute_hash(),
            base.with_umask(FileModeMask::new(0o077).unwrap())
                .compute_hash()
        );
        let patch = EnvPatch::new(BTreeMap::from([(
            key("MODE"),
            EnvEdit::set("release").unwrap(),
        )]));
        assert_ne!(base.compute_hash(), base.apply_env(&patch).compute_hash());
    }

    #[test]
    fn sensitivity_changes_scope_identity() {
        let base = scope();
        let patch = EnvPatch::new(BTreeMap::from([(
            key("HOME"),
            EnvEdit::Set(
                EnvValue::classified("/home/user", super::super::Sensitivity::Sensitive).unwrap(),
            ),
        )]));
        assert_ne!(base.compute_hash(), base.apply_env(&patch).compute_hash());
    }

    #[test]
    fn rejects_relative_paths_and_invalid_masks_at_json_boundary() {
        assert!(AbsolutePath::new("relative").is_err());
        assert!(AbsolutePath::new("/tmp/a\0b").is_err());
        assert!(FileModeMask::new(0o1000).is_err());
        assert!(serde_json::from_str::<AbsolutePath>(r#""relative""#).is_err());
        assert!(serde_json::from_str::<AbsolutePath>(r#""/tmp/a\u0000b""#).is_err());
        assert!(serde_json::from_str::<FileModeMask>("512").is_err());
    }

    #[test]
    fn scope_json_is_a_full_snapshot_without_parent_or_delta() {
        let encoded = serde_json::to_value(scope()).unwrap();
        assert_eq!(encoded["cwd"], "/workspace");
        assert_eq!(encoded["umask"], 0o022);
        assert!(encoded.get("parent").is_none());
        assert!(encoded.get("delta").is_none());
        assert!(encoded.get("hash").is_none());
    }
}
