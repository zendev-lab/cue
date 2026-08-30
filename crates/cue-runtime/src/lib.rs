//! Bootstrap-time composition for Cue runtime implementations.
//!
//! `cue-core` fixes what an execution means. This crate resolves which runtime
//! providers realize the required ports before the daemon becomes ready. The
//! resulting [`Assembly`] is a bootstrap artifact; execution code receives
//! typed fields built from it rather than looking services up dynamically.

mod composition;
mod ports;

pub use composition::{
    Assembly, AssemblyManifest, Combine, Composition, CompositionError, PortId, PortSpec,
    ProviderId, ProviderManifest, ProviderSpec, ResolvedPort,
};
pub use ports::{RuntimePort, canonical_port_specs, runtime_root_ports};
