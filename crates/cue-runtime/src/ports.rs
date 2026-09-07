use crate::{Combine, PortId, PortSpec};

/// Ports required or optionally consumed by the runtime root.
///
/// Extensions may register additional private ports for their own dependency
/// graphs. Only this closed set is projected into Cue's typed runtime Assembly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimePort {
    ExecutionStore,
    ScopeStore,
    OutputStore,
    ProcessSpawner,
    Workspace,
    SpawnTransform,
    SpawnGuard,
    ExecutionObserver,
}

impl RuntimePort {
    pub const ALL: [Self; 8] = [
        Self::ExecutionStore,
        Self::ScopeStore,
        Self::OutputStore,
        Self::ProcessSpawner,
        Self::Workspace,
        Self::SpawnTransform,
        Self::SpawnGuard,
        Self::ExecutionObserver,
    ];

    pub const fn id(self) -> &'static str {
        match self {
            Self::ExecutionStore => "execution_store",
            Self::ScopeStore => "scope_store",
            Self::OutputStore => "output_store",
            Self::ProcessSpawner => "process_spawner",
            Self::Workspace => "workspace",
            Self::SpawnTransform => "spawn_transform",
            Self::SpawnGuard => "spawn_guard",
            Self::ExecutionObserver => "execution_observer",
        }
    }

    pub const fn combine(self) -> Combine {
        match self {
            Self::ExecutionStore | Self::ScopeStore | Self::OutputStore | Self::ProcessSpawner => {
                Combine::ExactlyOne
            }
            Self::Workspace => Combine::ZeroOrOne,
            Self::SpawnTransform => Combine::Chain,
            Self::SpawnGuard => Combine::All,
            Self::ExecutionObserver => Combine::Fanout,
        }
    }

    pub fn port_id(self) -> PortId {
        PortId::new(self.id()).expect("canonical runtime port identifier")
    }

    pub fn specification(self) -> PortSpec {
        PortSpec::new(self.port_id(), self.combine())
    }
}

pub fn canonical_port_specs() -> Vec<PortSpec> {
    RuntimePort::ALL
        .into_iter()
        .map(RuntimePort::specification)
        .collect()
}

/// Every canonical port is a runtime root. Optional and multi ports resolve to
/// an empty contribution set when no implementation is enabled.
pub fn runtime_root_ports() -> Vec<PortId> {
    RuntimePort::ALL
        .into_iter()
        .map(RuntimePort::port_id)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_ports_lock_the_public_combine_laws() {
        let actual = RuntimePort::ALL
            .map(|port| (port.id(), port.combine()))
            .to_vec();
        assert_eq!(
            actual,
            vec![
                ("execution_store", Combine::ExactlyOne),
                ("scope_store", Combine::ExactlyOne),
                ("output_store", Combine::ExactlyOne),
                ("process_spawner", Combine::ExactlyOne),
                ("workspace", Combine::ZeroOrOne),
                ("spawn_transform", Combine::Chain),
                ("spawn_guard", Combine::All),
                ("execution_observer", Combine::Fanout),
            ]
        );
    }
}
