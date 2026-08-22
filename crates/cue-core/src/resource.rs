//! Resource scheduling shared types.
//!
//! This module defines the **schema-less** data model used by both the
//! daemon-side `ProviderRegistry` and the parser/IPC layer. The core does not
//! recognise specific resource keys (no `gpu`, no `tpu`, no `mem`); it only
//! provides:
//!
//! * [`ProviderId`] — newtype-wrapped string identifying a registered
//!   provider.
//! * [`ResourceQuantity`] — a quantity with two flavours: `Count(u64)` for
//!   integer counts (e.g. `1` GPU, `2` license tokens) and `Bytes(u64)` for
//!   memory-like resources (e.g. `24GiB` of GPU memory). It accepts IEC
//!   (`KiB`, `MiB`, `GiB`, `TiB`, or the bare `Ki/Mi/Gi/Ti` shorthands) and
//!   SI (`KB`, `MB`, `GB`, `TB`, `K/M/G/T`) suffixes.
//! * [`Need`] — opaque map `BTreeMap<String, ResourceQuantity>` collected
//!   from `:run(need.X=Y)` mode params. The daemon's provider registry is
//!   the only thing that knows how to interpret keys.
//! * [`ResourceUnit`] / [`Snapshot`] — what a provider reports back when
//!   asked for current capacity (e.g. one entry per GPU, one entry per
//!   licence pool).
//! * [`Reservation`] / [`Grant`] / [`Reject`] — the result of a single
//!   `try_reserve` call.
//!
//! The module is dependency-free aside from `serde`, `thiserror`, and the
//! existing `crate::id::StepId`. All types `derive` `Clone + Debug + Serialize
//! + Deserialize` so they can flow through IPC unchanged.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::id::StepId;

// ---------------------------------------------------------------------------
// ProviderId
// ---------------------------------------------------------------------------

/// Identifier of a registered resource provider, e.g. `"gpu"` or `"tpu"`.
///
/// Newtype around `String` rather than `&'static str` so providers can be
/// configured at runtime via TOML.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProviderId(pub String);

impl ProviderId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProviderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for ProviderId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for ProviderId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

// ---------------------------------------------------------------------------
// ReservationId
// ---------------------------------------------------------------------------

/// Identifier of a single reservation. Format and uniqueness are the
/// provider's responsibility (e.g. NVML provider may use
/// `gpu-<step_id>-<random>`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReservationId(pub String);

impl ReservationId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ReservationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ---------------------------------------------------------------------------
// ResourceQuantity
// ---------------------------------------------------------------------------

/// A typed resource quantity.
///
/// `Count` is for unitless integer counts ("1 GPU", "4 license tokens").
/// `Bytes` is for memory-like quantities (always stored as bytes internally,
/// regardless of the suffix used at parse time).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ResourceQuantity {
    Count(u64),
    Bytes(u64),
}

impl ResourceQuantity {
    /// Sum two quantities of the same flavour. Returns `None` on flavour
    /// mismatch or overflow.
    pub fn checked_add(self, other: Self) -> Option<Self> {
        match (self, other) {
            (Self::Count(a), Self::Count(b)) => a.checked_add(b).map(Self::Count),
            (Self::Bytes(a), Self::Bytes(b)) => a.checked_add(b).map(Self::Bytes),
            _ => None,
        }
    }

    /// Saturating subtraction within the same flavour. Mismatched flavours
    /// return `None`.
    pub fn saturating_sub(self, other: Self) -> Option<Self> {
        match (self, other) {
            (Self::Count(a), Self::Count(b)) => Some(Self::Count(a.saturating_sub(b))),
            (Self::Bytes(a), Self::Bytes(b)) => Some(Self::Bytes(a.saturating_sub(b))),
            _ => None,
        }
    }

    /// Returns the byte value if this quantity is a `Bytes` variant.
    pub fn as_bytes(self) -> Option<u64> {
        match self {
            Self::Bytes(n) => Some(n),
            Self::Count(_) => None,
        }
    }

    /// Returns the count value if this quantity is a `Count` variant.
    pub fn as_count(self) -> Option<u64> {
        match self {
            Self::Count(n) => Some(n),
            Self::Bytes(_) => None,
        }
    }

    /// Quantity flavour (count vs bytes), used for diagnostics.
    pub fn flavour(self) -> &'static str {
        match self {
            Self::Count(_) => "count",
            Self::Bytes(_) => "bytes",
        }
    }
}

impl fmt::Display for ResourceQuantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Count(n) => write!(f, "{n}"),
            Self::Bytes(n) => write!(f, "{}", format_bytes_iec(n)),
        }
    }
}

/// Format a byte count using the largest exact IEC unit, falling back to
/// bytes when no larger unit fits cleanly.
fn format_bytes_iec(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    const TIB: u64 = 1024 * GIB;

    if bytes == 0 {
        return "0B".to_string();
    }

    if bytes.is_multiple_of(TIB) {
        return format!("{}TiB", bytes / TIB);
    }
    if bytes.is_multiple_of(GIB) {
        return format!("{}GiB", bytes / GIB);
    }
    if bytes.is_multiple_of(MIB) {
        return format!("{}MiB", bytes / MIB);
    }
    if bytes.is_multiple_of(KIB) {
        return format!("{}KiB", bytes / KIB);
    }
    format!("{bytes}B")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseQuantityError {
    pub input: String,
    pub reason: ParseQuantityReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseQuantityReason {
    Empty,
    InvalidNumber,
    InvalidSuffix(String),
    Overflow,
}

impl fmt::Display for ParseQuantityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.reason {
            ParseQuantityReason::Empty => write!(f, "empty resource quantity"),
            ParseQuantityReason::InvalidNumber => {
                write!(
                    f,
                    "resource quantity {:?} has no leading integer",
                    self.input
                )
            }
            ParseQuantityReason::InvalidSuffix(s) => {
                write!(
                    f,
                    "resource quantity {:?} has unknown unit suffix {:?}",
                    self.input, s
                )
            }
            ParseQuantityReason::Overflow => {
                write!(f, "resource quantity {:?} overflows u64", self.input)
            }
        }
    }
}

impl Error for ParseQuantityError {}

impl FromStr for ResourceQuantity {
    type Err = ParseQuantityError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(ParseQuantityError {
                input: input.to_owned(),
                reason: ParseQuantityReason::Empty,
            });
        }

        // Split into [leading digits] [suffix].
        let split = trimmed
            .find(|ch: char| !ch.is_ascii_digit() && ch != '_')
            .unwrap_or(trimmed.len());
        let (num_part, suffix_raw) = trimmed.split_at(split);
        let suffix = suffix_raw.trim();

        if num_part.is_empty() {
            return Err(ParseQuantityError {
                input: input.to_owned(),
                reason: ParseQuantityReason::InvalidNumber,
            });
        }
        let cleaned: String = num_part.chars().filter(|&c| c != '_').collect();
        let value: u64 = cleaned.parse().map_err(|_| ParseQuantityError {
            input: input.to_owned(),
            reason: ParseQuantityReason::InvalidNumber,
        })?;

        if suffix.is_empty() {
            return Ok(Self::Count(value));
        }

        let multiplier = unit_multiplier(suffix).ok_or_else(|| ParseQuantityError {
            input: input.to_owned(),
            reason: ParseQuantityReason::InvalidSuffix(suffix.to_owned()),
        })?;
        let bytes = value
            .checked_mul(multiplier)
            .ok_or_else(|| ParseQuantityError {
                input: input.to_owned(),
                reason: ParseQuantityReason::Overflow,
            })?;
        Ok(Self::Bytes(bytes))
    }
}

fn unit_multiplier(suffix: &str) -> Option<u64> {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    const TIB: u64 = 1024 * GIB;
    const PIB: u64 = 1024 * TIB;
    const KB: u64 = 1000;
    const MB: u64 = 1000 * KB;
    const GB: u64 = 1000 * MB;
    const TB: u64 = 1000 * GB;
    const PB: u64 = 1000 * TB;

    match suffix {
        // Bare bytes suffix
        "B" | "b" => Some(1),
        // IEC binary
        "KiB" | "Ki" | "kiB" | "ki" => Some(KIB),
        "MiB" | "Mi" | "miB" | "mi" => Some(MIB),
        "GiB" | "Gi" | "giB" | "gi" => Some(GIB),
        "TiB" | "Ti" | "tiB" | "ti" => Some(TIB),
        "PiB" | "Pi" | "piB" | "pi" => Some(PIB),
        // SI decimal (kB/KB are both treated as 1000 — the unambiguous
        // shape; users who mean 1024 should write KiB).
        "KB" | "kB" | "K" | "k" => Some(KB),
        "MB" | "M" => Some(MB),
        "GB" | "G" => Some(GB),
        "TB" | "T" => Some(TB),
        "PB" | "P" => Some(PB),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Need
// ---------------------------------------------------------------------------

/// Opaque set of resource requests collected from a job's `need.X=Y` mode
/// params.
///
/// Keys are routed by `ProviderRegistry` to providers; the core does not
/// validate them. Iteration order is stable (`BTreeMap`), so downstream
/// reservations and `pending_reason` strings stay deterministic.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Need(BTreeMap<String, ResourceQuantity>);

impl Need {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a `Need` from an iterator of `(key, quantity)` pairs.
    /// Inherent constructor (we deliberately do **not** implement
    /// `FromIterator` to avoid name resolution ambiguity).
    pub fn from_pairs<I, K>(iter: I) -> Self
    where
        I: IntoIterator<Item = (K, ResourceQuantity)>,
        K: Into<String>,
    {
        Self(iter.into_iter().map(|(k, v)| (k.into(), v)).collect())
    }

    pub fn insert(&mut self, key: impl Into<String>, value: ResourceQuantity) {
        self.0.insert(key.into(), value);
    }

    pub fn get(&self, key: &str) -> Option<ResourceQuantity> {
        self.0.get(key).copied()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, ResourceQuantity)> {
        self.0.iter().map(|(k, v)| (k.as_str(), *v))
    }

    /// View the underlying map. Public so the daemon can borrow without
    /// copying when routing keys to providers.
    pub fn as_map(&self) -> &BTreeMap<String, ResourceQuantity> {
        &self.0
    }

    pub fn into_map(self) -> BTreeMap<String, ResourceQuantity> {
        self.0
    }

    /// Return a `Need` containing only the entries whose keys are present in
    /// `selected`. Used by the registry to dispatch a key subset to one
    /// provider.
    pub fn select(&self, selected: &[&str]) -> Self {
        let mut out = BTreeMap::new();
        for &k in selected {
            if let Some(&v) = self.0.get(k) {
                out.insert(k.to_owned(), v);
            }
        }
        Self(out)
    }
}

// (No `FromIterator` impl: the inherent `Need::from_pairs` is the canonical
// constructor.)

// ---------------------------------------------------------------------------
// ResourceUnit & Snapshot
// ---------------------------------------------------------------------------

/// One indivisible resource unit reported by a provider, e.g. one GPU card or
/// one license-pool. `attrs` keys are provider-defined (`free_mem`,
/// `total_mem`, `gpu_util`, ...).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceUnit {
    pub id: String,
    pub attrs: BTreeMap<String, ResourceQuantity>,
}

impl ResourceUnit {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            attrs: BTreeMap::new(),
        }
    }

    pub fn with_attr(mut self, key: impl Into<String>, value: ResourceQuantity) -> Self {
        self.attrs.insert(key.into(), value);
        self
    }
}

/// Capacity / availability snapshot for a provider, used to back `:resources`
/// and the admission formula.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub provider_id: ProviderId,
    pub units: Vec<ResourceUnit>,
    /// Wall-clock time at which the snapshot was taken (used for TTL-based
    /// caching by the providers themselves).
    pub captured_at: SystemTime,
}

impl Snapshot {
    pub fn new(provider_id: impl Into<ProviderId>, units: Vec<ResourceUnit>) -> Self {
        Self {
            provider_id: provider_id.into(),
            units,
            captured_at: SystemTime::now(),
        }
    }

    /// Construct a snapshot with an explicit capture time. Useful for tests
    /// and for reproducing snapshots from IPC payloads.
    pub fn at(
        provider_id: impl Into<ProviderId>,
        units: Vec<ResourceUnit>,
        captured_at: SystemTime,
    ) -> Self {
        Self {
            provider_id: provider_id.into(),
            units,
            captured_at,
        }
    }
}

// ---------------------------------------------------------------------------
// Reservation / Grant / Reject
// ---------------------------------------------------------------------------

/// A single successful reservation returned by `Provider::reserve`.
///
/// Owned by the daemon's `ProviderRegistry` and persisted in memory until the
/// owning step reaches a terminal state. `env` is merged into the spawned
/// scope; `info` carries provider-specific bookkeeping (e.g. reserved bytes
/// per GPU index) for `:resources` output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reservation {
    pub id: ReservationId,
    pub step_id: StepId,
    pub provider_id: ProviderId,
    /// Environment overrides that must be applied to the spawned scope
    /// (e.g. `CUDA_VISIBLE_DEVICES`, `CUDA_DEVICE_ORDER`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Provider-defined bookkeeping payload (free-form quantity map).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub info: BTreeMap<String, ResourceQuantity>,
    /// Wall-clock acquire time, serialised as a Unix-epoch millisecond
    /// integer for IPC stability.
    #[serde(with = "serde_systemtime_millis")]
    pub acquired_at: SystemTime,
}

impl Reservation {
    pub fn new(
        id: impl Into<ReservationId>,
        step_id: StepId,
        provider_id: impl Into<ProviderId>,
    ) -> Self {
        Self {
            id: id.into(),
            step_id,
            provider_id: provider_id.into(),
            env: BTreeMap::new(),
            info: BTreeMap::new(),
            acquired_at: SystemTime::now(),
        }
    }

    pub fn with_env(mut self, env: BTreeMap<String, String>) -> Self {
        self.env = env;
        self
    }

    pub fn with_info(mut self, info: BTreeMap<String, ResourceQuantity>) -> Self {
        self.info = info;
        self
    }
}

impl From<&str> for ReservationId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for ReservationId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// `reserve()` success payload. Conceptually identical to a single
/// `Reservation`; aliased so call-sites can read intent at a glance:
/// `Provider::reserve(req) -> Result<Grant, Reject>`.
pub type Grant = Reservation;

/// Reason why a reservation request was rejected.
///
/// `reason` is a human-readable string surfaced via `pending_reason`;
/// `needed` and `available` are optional structured hints that allow the CLI
/// surface to format precise messages (e.g. `"need 24GiB, max effective free
/// 18GiB"`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reject {
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub needed: Option<ResourceQuantity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available: Option<ResourceQuantity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

impl Reject {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            needed: None,
            available: None,
            key: None,
        }
    }

    pub fn with_demand(mut self, key: impl Into<String>, needed: ResourceQuantity) -> Self {
        self.key = Some(key.into());
        self.needed = Some(needed);
        self
    }

    pub fn with_available(mut self, available: ResourceQuantity) -> Self {
        self.available = Some(available);
        self
    }
}

impl fmt::Display for Reject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.reason)
    }
}

impl Error for Reject {}

// ---------------------------------------------------------------------------
// SystemTime serde helper
// ---------------------------------------------------------------------------

mod serde_systemtime_millis {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(t: &SystemTime, ser: S) -> Result<S::Ok, S::Error> {
        let millis: i64 = match t.duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_millis() as i64,
            Err(e) => -(e.duration().as_millis() as i64),
        };
        ser.serialize_i64(millis)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<SystemTime, D::Error> {
        let raw = i64::deserialize(de)?;
        let st = if raw >= 0 {
            UNIX_EPOCH + Duration::from_millis(raw as u64)
        } else {
            UNIX_EPOCH - Duration::from_millis((-raw) as u64)
        };
        Ok(st)
    }
}

/// Convenience helper used in tests and diagnostics.
pub fn unix_millis(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(e) => -(e.duration().as_millis() as i64),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn count(n: u64) -> ResourceQuantity {
        ResourceQuantity::Count(n)
    }

    fn bytes(n: u64) -> ResourceQuantity {
        ResourceQuantity::Bytes(n)
    }

    // -- ResourceQuantity ---------------------------------------------------

    #[test]
    fn parse_bare_integer_yields_count() {
        assert_eq!("0".parse::<ResourceQuantity>().unwrap(), count(0));
        assert_eq!("42".parse::<ResourceQuantity>().unwrap(), count(42));
        assert_eq!("1".parse::<ResourceQuantity>().unwrap(), count(1));
    }

    #[test]
    fn parse_iec_suffix_yields_bytes() {
        assert_eq!("1KiB".parse::<ResourceQuantity>().unwrap(), bytes(1024));
        assert_eq!("1Ki".parse::<ResourceQuantity>().unwrap(), bytes(1024));
        assert_eq!(
            "24GiB".parse::<ResourceQuantity>().unwrap(),
            bytes(24 * 1024 * 1024 * 1024)
        );
        assert_eq!(
            "2TiB".parse::<ResourceQuantity>().unwrap(),
            bytes(2u64 * 1024 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn parse_si_suffix_yields_decimal_bytes() {
        assert_eq!("1KB".parse::<ResourceQuantity>().unwrap(), bytes(1000));
        assert_eq!("1MB".parse::<ResourceQuantity>().unwrap(), bytes(1_000_000));
        assert_eq!(
            "1GB".parse::<ResourceQuantity>().unwrap(),
            bytes(1_000_000_000)
        );
        // Bare uppercase letter is treated as SI for consistency with `KB`.
        assert_eq!("1K".parse::<ResourceQuantity>().unwrap(), bytes(1000));
        assert_eq!("1B".parse::<ResourceQuantity>().unwrap(), bytes(1));
    }

    #[test]
    fn parse_with_underscores_and_whitespace() {
        assert_eq!(
            "1_000_000".parse::<ResourceQuantity>().unwrap(),
            count(1_000_000)
        );
        assert_eq!(
            " 24GiB ".parse::<ResourceQuantity>().unwrap(),
            bytes(24 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn parse_rejects_empty_and_garbage() {
        assert!("".parse::<ResourceQuantity>().is_err());
        assert!("   ".parse::<ResourceQuantity>().is_err());
        assert!("abc".parse::<ResourceQuantity>().is_err());
        assert!("1XB".parse::<ResourceQuantity>().is_err());
        assert!("Gi".parse::<ResourceQuantity>().is_err());
    }

    #[test]
    fn parse_overflow_is_reported() {
        // 18446744073709 GiB ≫ u64::MAX
        let err = "18446744073709GiB".parse::<ResourceQuantity>().unwrap_err();
        assert!(matches!(err.reason, ParseQuantityReason::Overflow));
    }

    #[test]
    fn display_roundtrip_for_bytes() {
        let cases = [
            ("0B", bytes(0)),
            ("512B", bytes(512)),
            ("1KiB", bytes(1024)),
            ("24GiB", bytes(24 * 1024 * 1024 * 1024)),
            ("2TiB", bytes(2u64 * 1024 * 1024 * 1024 * 1024)),
        ];
        for (text, q) in cases {
            assert_eq!(q.to_string(), text, "display {q:?}");
            assert_eq!(text.parse::<ResourceQuantity>().unwrap(), q, "parse {text}");
        }
    }

    #[test]
    fn display_count_is_bare_number() {
        assert_eq!(count(0).to_string(), "0");
        assert_eq!(count(7).to_string(), "7");
    }

    #[test]
    fn checked_add_respects_flavour() {
        assert_eq!(count(1).checked_add(count(2)), Some(count(3)));
        assert_eq!(bytes(1024).checked_add(bytes(1024)), Some(bytes(2048)));
        assert_eq!(count(1).checked_add(bytes(1)), None);
        assert_eq!(bytes(u64::MAX).checked_add(bytes(1)), None);
    }

    #[test]
    fn flavour_helpers() {
        assert_eq!(count(1).flavour(), "count");
        assert_eq!(bytes(1).flavour(), "bytes");
        assert_eq!(count(7).as_count(), Some(7));
        assert_eq!(count(7).as_bytes(), None);
        assert_eq!(bytes(7).as_bytes(), Some(7));
        assert_eq!(bytes(7).as_count(), None);
    }

    // -- Need ---------------------------------------------------------------

    #[test]
    fn need_iteration_is_sorted_for_serialisation_stability() {
        let n = Need::from_pairs([
            ("gpu_mem", bytes(24 * 1024 * 1024 * 1024)),
            ("gpu", count(1)),
        ]);
        // BTreeMap orders keys alphabetically; gpu < gpu_mem.
        let keys: Vec<_> = n.keys().collect();
        assert_eq!(keys, vec!["gpu", "gpu_mem"]);

        // serde JSON output reflects the same ordering.
        let json = serde_json::to_string(&n).unwrap();
        assert!(
            json.find("\"gpu\"").unwrap() < json.find("\"gpu_mem\"").unwrap(),
            "serialised keys must keep BTreeMap order: {json}",
        );
    }

    #[test]
    fn need_select_returns_only_matching_keys() {
        let n = Need::from_pairs([
            ("gpu", count(1)),
            ("gpu_mem", bytes(24 * 1024 * 1024 * 1024)),
            ("tpu", count(2)),
        ]);
        let gpu_only = n.select(&["gpu", "gpu_mem"]);
        assert_eq!(
            gpu_only.as_map().keys().cloned().collect::<Vec<_>>(),
            vec!["gpu".to_string(), "gpu_mem".to_string()],
        );
        assert!(!gpu_only.contains("tpu"));
    }

    #[test]
    fn need_serde_roundtrip() {
        let n = Need::from_pairs([
            ("gpu", count(2)),
            ("gpu_mem", bytes(24 * 1024 * 1024 * 1024)),
        ]);
        let json = serde_json::to_string(&n).unwrap();
        let back: Need = serde_json::from_str(&json).unwrap();
        assert_eq!(n, back);
    }

    // -- Reservation / Reject ----------------------------------------------

    #[test]
    fn reservation_serde_roundtrip_preserves_fields() {
        let mut r = Reservation::new(
            ReservationId::new("g-1"),
            StepId {
                execution: crate::ExecutionId(7),
                index: 1,
            },
            ProviderId::new("gpu"),
        );
        r.env.insert("CUDA_VISIBLE_DEVICES".into(), "0,2".into());
        r.info.insert(
            "reserved_mem".into(),
            ResourceQuantity::Bytes(24 * 1024 * 1024 * 1024),
        );
        // Pin acquired_at so the millisecond rounding is deterministic.
        r.acquired_at = UNIX_EPOCH + std::time::Duration::from_millis(1_700_000_000_123);

        let json = serde_json::to_string(&r).unwrap();
        let back: Reservation = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, r.id);
        assert_eq!(back.step_id, r.step_id);
        assert_eq!(back.provider_id, r.provider_id);
        assert_eq!(back.env, r.env);
        assert_eq!(back.info, r.info);
        // Roundtrip through millisecond integer must preserve millis exactly.
        assert_eq!(unix_millis(back.acquired_at), unix_millis(r.acquired_at));
    }

    #[test]
    fn reservation_skips_empty_optional_maps_when_serialised() {
        let r = Reservation::new(
            "g-2",
            StepId {
                execution: crate::ExecutionId(1),
                index: 1,
            },
            "gpu",
        );
        let json = serde_json::to_string(&r).unwrap();
        assert!(
            !json.contains("\"env\""),
            "empty env should be skipped: {json}"
        );
        assert!(
            !json.contains("\"info\""),
            "empty info should be skipped: {json}"
        );
    }

    #[test]
    fn reject_carries_demand_summary() {
        let r = Reject::new("waiting GPU")
            .with_demand("gpu_mem", bytes(24 * 1024 * 1024 * 1024))
            .with_available(bytes(18 * 1024 * 1024 * 1024));
        assert_eq!(r.reason, "waiting GPU");
        assert_eq!(r.key.as_deref(), Some("gpu_mem"));
        assert_eq!(r.needed, Some(bytes(24 * 1024 * 1024 * 1024)));
        assert_eq!(r.available, Some(bytes(18 * 1024 * 1024 * 1024)));

        let json = serde_json::to_string(&r).unwrap();
        let back: Reject = serde_json::from_str(&json).unwrap();
        assert_eq!(back, r);
    }

    // -- Snapshot / ResourceUnit -------------------------------------------

    #[test]
    fn snapshot_at_preserves_capture_time() {
        let when = UNIX_EPOCH + std::time::Duration::from_millis(123_456_789);
        let snap = Snapshot::at(
            "gpu",
            vec![ResourceUnit::new("gpu0").with_attr("free_mem", bytes(1024))],
            when,
        );
        let json = serde_json::to_string(&snap).unwrap();
        let back: Snapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back.provider_id, ProviderId::new("gpu"));
        assert_eq!(back.units, snap.units);
        // SystemTime serde isn't ours here; rely on stdlib roundtrip.
        assert_eq!(back.captured_at, snap.captured_at);
    }
}
