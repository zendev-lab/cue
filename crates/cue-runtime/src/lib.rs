//! Bootstrap-time composition for Cue runtime implementations.
//!
//! `cue-core` fixes what an execution means. This crate resolves which runtime
//! providers realize the required ports before the daemon becomes ready. The
//! resulting [`Assembly`] is a bootstrap artifact; execution code receives
//! typed fields built from it rather than looking services up dynamically.

mod composition;
mod output;
mod ports;
mod provider;
mod recovery;
mod runner;

pub use composition::{
    Assembly, AssemblyManifest, Combine, Composition, CompositionError, PortId, PortSpec,
    ProviderId, ProviderManifest, ProviderSpec, ResolvedPort,
};
pub use output::{DEFAULT_OUTPUT_CAPACITY, MemoryOutputStore};
pub use ports::{RuntimePort, canonical_port_specs, runtime_root_ports};
pub use provider::{
    AssemblyBindingError, ExecutionObserver, ExecutionStore, OutputAppend, OutputSlice,
    OutputStore, ProcessSpawner, ProviderBundle, ProviderRegistry, RunControl, RunExit,
    RuntimeAssembly, RuntimeError, RuntimeErrorKind, RuntimeFuture, ScopeDurability, ScopeStore,
    SpawnGuard, SpawnRequest, SpawnTransform, SpawnedRun, TerminalSize, Workspace,
};
pub use recovery::{RecoveryCommit, recover_interrupted};
pub use runner::LocalProcessSpawner;
