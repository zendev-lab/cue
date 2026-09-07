use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::Serialize;
use thiserror::Error;

type ProviderEdges = BTreeSet<(ProviderId, ProviderId)>;

/// Stable identifier for a runtime capability.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct PortId(String);

impl PortId {
    pub fn new(value: impl Into<String>) -> Result<Self, CompositionError> {
        let value = value.into();
        validate_identifier("port", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PortId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Stable identifier for one provider implementation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ProviderId(String);

impl ProviderId {
    pub fn new(value: impl Into<String>) -> Result<Self, CompositionError> {
        let value = value.into();
        validate_identifier("provider", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProviderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// How contributions to one port are combined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Combine {
    /// Exactly one enabled provider must contribute.
    ExactlyOne,
    /// Zero or one enabled provider may contribute.
    ZeroOrOne,
    /// Every provider contributes to a deterministic transform chain.
    Chain,
    /// Every provider must approve the operation.
    All,
    /// Every provider observes the committed fact.
    Fanout,
}

impl Combine {
    fn allows_many(self) -> bool {
        matches!(self, Self::Chain | Self::All | Self::Fanout)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortSpec {
    id: PortId,
    combine: Combine,
}

impl PortSpec {
    pub fn new(id: PortId, combine: Combine) -> Self {
        Self { id, combine }
    }

    pub fn id(&self) -> &PortId {
        &self.id
    }

    pub fn combine(&self) -> Combine {
        self.combine
    }
}

/// Bootstrap metadata for one enabled provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderSpec {
    id: ProviderId,
    version: String,
    provides: BTreeSet<PortId>,
    requires: BTreeSet<PortId>,
    before: BTreeMap<PortId, BTreeSet<ProviderId>>,
    after: BTreeMap<PortId, BTreeSet<ProviderId>>,
}

impl ProviderSpec {
    pub fn new(
        id: ProviderId,
        version: impl Into<String>,
        provides: impl IntoIterator<Item = PortId>,
    ) -> Result<Self, CompositionError> {
        let version = version.into();
        if version.trim().is_empty() {
            return Err(CompositionError::InvalidProvider {
                provider: id,
                message: "version must not be empty".into(),
            });
        }
        let provides = provides.into_iter().collect::<BTreeSet<_>>();
        if provides.is_empty() {
            return Err(CompositionError::InvalidProvider {
                provider: id,
                message: "provider must contribute at least one port".into(),
            });
        }
        Ok(Self {
            id,
            version,
            provides,
            requires: BTreeSet::new(),
            before: BTreeMap::new(),
            after: BTreeMap::new(),
        })
    }

    pub fn require(mut self, port: PortId) -> Self {
        self.requires.insert(port);
        self
    }

    pub fn before(mut self, port: PortId, provider: ProviderId) -> Self {
        self.before.entry(port).or_default().insert(provider);
        self
    }

    pub fn after(mut self, port: PortId, provider: ProviderId) -> Self {
        self.after.entry(port).or_default().insert(provider);
        self
    }

    pub fn id(&self) -> &ProviderId {
        &self.id
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn provides(&self) -> impl Iterator<Item = &PortId> {
        self.provides.iter()
    }

    pub fn requires(&self) -> impl Iterator<Item = &PortId> {
        self.requires.iter()
    }
}

/// Mutable bootstrap description. It is consumed by [`Composition::resolve`]
/// before any runtime work is admitted.
#[derive(Debug, Default)]
pub struct Composition {
    ports: BTreeMap<PortId, PortSpec>,
    providers: BTreeMap<ProviderId, ProviderSpec>,
}

impl Composition {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_port(&mut self, port: PortSpec) -> Result<(), CompositionError> {
        if self.ports.contains_key(port.id()) {
            return Err(CompositionError::DuplicatePort(port.id().clone()));
        }
        self.ports.insert(port.id().clone(), port);
        Ok(())
    }

    pub fn register_provider(&mut self, provider: ProviderSpec) -> Result<(), CompositionError> {
        if self.providers.contains_key(provider.id()) {
            return Err(CompositionError::DuplicateProvider(provider.id().clone()));
        }
        self.providers.insert(provider.id().clone(), provider);
        Ok(())
    }

    /// Resolve the ports required by the runtime root into one deterministic
    /// bootstrap assembly.
    pub fn resolve(
        &self,
        roots: impl IntoIterator<Item = PortId>,
    ) -> Result<Assembly, CompositionError> {
        self.validate_references()?;

        let roots = roots.into_iter().collect::<BTreeSet<_>>();
        for root in &roots {
            if !self.ports.contains_key(root) {
                return Err(CompositionError::UnknownRootPort(root.clone()));
            }
        }

        let mut selected_providers = BTreeSet::new();
        let mut selected_ports = BTreeMap::<PortId, Vec<ProviderId>>::new();
        let mut pending_ports = roots;
        let mut resolved_ports = BTreeSet::new();

        while let Some(port_id) = pending_ports.pop_first() {
            if !resolved_ports.insert(port_id.clone()) {
                continue;
            }
            let port = &self.ports[&port_id];
            let providers = self.contributors(&port_id);
            let selected = select_contributors(port, providers)?;
            for provider_id in &selected {
                if selected_providers.insert(provider_id.clone()) {
                    pending_ports.extend(self.providers[provider_id].requires.iter().cloned());
                }
            }
            selected_ports.insert(port_id, selected);
        }

        let mut dependency_edges = ProviderEdges::new();
        for provider_id in &selected_providers {
            for required in &self.providers[provider_id].requires {
                for dependency in &selected_ports[required] {
                    dependency_edges.insert((dependency.clone(), provider_id.clone()));
                }
            }
        }

        let mut resolved = BTreeMap::new();
        for (port_id, contributors) in selected_ports {
            let port = &self.ports[&port_id];
            let ordered = if port.combine().allows_many() {
                self.order_contributors(&port_id, &contributors)?
            } else {
                contributors
            };
            resolved.insert(
                port_id.clone(),
                ResolvedPort {
                    id: port_id,
                    combine: port.combine(),
                    providers: ordered,
                },
            );
        }

        let initialization_order = topological_order(&selected_providers, &dependency_edges)
            .map_err(|providers| CompositionError::DependencyCycle { providers })?;

        let provider_manifest = selected_providers
            .into_iter()
            .map(|provider_id| {
                let provider = &self.providers[&provider_id];
                (
                    provider_id.clone(),
                    ProviderManifest {
                        id: provider_id,
                        version: provider.version.clone(),
                    },
                )
            })
            .collect();

        Ok(Assembly {
            ports: resolved,
            providers: provider_manifest,
            initialization_order,
        })
    }

    fn validate_references(&self) -> Result<(), CompositionError> {
        for provider in self.providers.values() {
            for port in provider.provides.iter().chain(&provider.requires) {
                if !self.ports.contains_key(port) {
                    return Err(CompositionError::UnknownProviderPort {
                        provider: provider.id.clone(),
                        port: port.clone(),
                    });
                }
            }
            for (port, targets) in provider.before.iter().chain(&provider.after) {
                for target in targets {
                    let target_spec = self.providers.get(target).ok_or_else(|| {
                        CompositionError::UnknownOrderingProvider {
                            provider: provider.id.clone(),
                            target: target.clone(),
                        }
                    })?;
                    if !provider.provides.contains(port)
                        || !target_spec.provides.contains(port)
                        || !self
                            .ports
                            .get(port)
                            .is_some_and(|port| port.combine().allows_many())
                    {
                        return Err(CompositionError::InvalidOrderingTarget {
                            port: port.clone(),
                            provider: provider.id.clone(),
                            target: target.clone(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn contributors(&self, port: &PortId) -> Vec<ProviderId> {
        self.providers
            .values()
            .filter(|provider| provider.provides.contains(port))
            .map(|provider| provider.id.clone())
            .collect()
    }

    fn order_contributors(
        &self,
        port: &PortId,
        contributors: &[ProviderId],
    ) -> Result<Vec<ProviderId>, CompositionError> {
        let nodes = contributors.iter().cloned().collect::<BTreeSet<_>>();
        let mut edges = BTreeSet::new();
        for provider_id in contributors {
            let provider = &self.providers[provider_id];
            for target in provider.before.get(port).into_iter().flatten() {
                if !nodes.contains(target) {
                    return Err(CompositionError::InvalidOrderingTarget {
                        port: port.clone(),
                        provider: provider_id.clone(),
                        target: target.clone(),
                    });
                }
                edges.insert((provider_id.clone(), target.clone()));
            }
            for target in provider.after.get(port).into_iter().flatten() {
                if !nodes.contains(target) {
                    return Err(CompositionError::InvalidOrderingTarget {
                        port: port.clone(),
                        provider: provider_id.clone(),
                        target: target.clone(),
                    });
                }
                edges.insert((target.clone(), provider_id.clone()));
            }
        }
        let ordered = topological_order(&nodes, &edges).map_err(|providers| {
            CompositionError::OrderingCycle {
                port: port.clone(),
                providers,
            }
        })?;
        Ok(ordered)
    }
}

fn select_contributors(
    port: &PortSpec,
    contributors: Vec<ProviderId>,
) -> Result<Vec<ProviderId>, CompositionError> {
    match port.combine() {
        Combine::ExactlyOne if contributors.is_empty() => {
            Err(CompositionError::MissingProvider(port.id().clone()))
        }
        Combine::ExactlyOne | Combine::ZeroOrOne if contributors.len() > 1 => {
            Err(CompositionError::AmbiguousProvider {
                port: port.id().clone(),
                providers: contributors,
            })
        }
        _ => Ok(contributors),
    }
}

fn topological_order(
    nodes: &BTreeSet<ProviderId>,
    edges: &ProviderEdges,
) -> Result<Vec<ProviderId>, Vec<ProviderId>> {
    let mut indegree = nodes
        .iter()
        .cloned()
        .map(|node| (node, 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = BTreeMap::<ProviderId, BTreeSet<ProviderId>>::new();
    for (from, to) in edges {
        if !nodes.contains(from) || !nodes.contains(to) {
            continue;
        }
        if outgoing.entry(from.clone()).or_default().insert(to.clone()) {
            *indegree.get_mut(to).expect("edge target belongs to nodes") += 1;
        }
    }
    let mut ready = indegree
        .iter()
        .filter_map(|(node, degree)| (*degree == 0).then_some(node.clone()))
        .collect::<BTreeSet<_>>();
    let mut ordered = Vec::with_capacity(nodes.len());
    while let Some(node) = ready.pop_first() {
        ordered.push(node.clone());
        if let Some(next_nodes) = outgoing.get(&node) {
            for next in next_nodes {
                let degree = indegree
                    .get_mut(next)
                    .expect("edge target belongs to nodes");
                *degree -= 1;
                if *degree == 0 {
                    ready.insert(next.clone());
                }
            }
        }
    }
    if ordered.len() == nodes.len() {
        Ok(ordered)
    } else {
        Err(indegree
            .into_iter()
            .filter_map(|(node, degree)| (degree > 0).then_some(node))
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResolvedPort {
    pub id: PortId,
    pub combine: Combine,
    pub providers: Vec<ProviderId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderManifest {
    pub id: ProviderId,
    pub version: String,
}

/// Fully resolved bootstrap graph. Runtime code should immediately convert
/// this into typed fields and retain [`AssemblyManifest`] only for inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assembly {
    ports: BTreeMap<PortId, ResolvedPort>,
    providers: BTreeMap<ProviderId, ProviderManifest>,
    initialization_order: Vec<ProviderId>,
}

impl Assembly {
    pub fn port(&self, id: &PortId) -> Option<&ResolvedPort> {
        self.ports.get(id)
    }

    pub fn initialization_order(&self) -> &[ProviderId] {
        &self.initialization_order
    }

    pub fn manifest(&self) -> AssemblyManifest {
        AssemblyManifest {
            ports: self.ports.values().cloned().collect(),
            providers: self.providers.values().cloned().collect(),
            initialization_order: self.initialization_order.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AssemblyManifest {
    pub ports: Vec<ResolvedPort>,
    pub providers: Vec<ProviderManifest>,
    pub initialization_order: Vec<ProviderId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CompositionError {
    #[error("invalid {kind} identifier {value:?}")]
    InvalidIdentifier { kind: &'static str, value: String },
    #[error("invalid provider {provider}: {message}")]
    InvalidProvider {
        provider: ProviderId,
        message: String,
    },
    #[error("duplicate port {0}")]
    DuplicatePort(PortId),
    #[error("duplicate provider {0}")]
    DuplicateProvider(ProviderId),
    #[error("unknown runtime root port {0}")]
    UnknownRootPort(PortId),
    #[error("provider {provider} references unknown port {port}")]
    UnknownProviderPort { provider: ProviderId, port: PortId },
    #[error("port {0} requires exactly one provider, but none is enabled")]
    MissingProvider(PortId),
    #[error("port {port} has ambiguous providers: {providers:?}")]
    AmbiguousProvider {
        port: PortId,
        providers: Vec<ProviderId>,
    },
    #[error("provider {provider} orders itself relative to unknown provider {target}")]
    UnknownOrderingProvider {
        provider: ProviderId,
        target: ProviderId,
    },
    #[error(
        "provider {provider} orders itself relative to {target}, which does not contribute to port {port}"
    )]
    InvalidOrderingTarget {
        port: PortId,
        provider: ProviderId,
        target: ProviderId,
    },
    #[error("provider ordering for port {port} contains a cycle: {providers:?}")]
    OrderingCycle {
        port: PortId,
        providers: Vec<ProviderId>,
    },
    #[error("provider dependency graph contains a cycle: {providers:?}")]
    DependencyCycle { providers: Vec<ProviderId> },
}

fn validate_identifier(kind: &'static str, value: &str) -> Result<(), CompositionError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
    {
        return Err(CompositionError::InvalidIdentifier {
            kind,
            value: value.to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn port(value: &str) -> PortId {
        PortId::new(value).unwrap()
    }

    fn provider(value: &str) -> ProviderId {
        ProviderId::new(value).unwrap()
    }

    fn provide(id: &str, ports: &[&str]) -> ProviderSpec {
        ProviderSpec::new(provider(id), "1", ports.iter().map(|value| port(value))).unwrap()
    }

    #[test]
    fn resolves_exact_provider_and_private_dependency_before_runtime_root() {
        let mut composition = Composition::new();
        composition
            .register_port(PortSpec::new(port("process_spawner"), Combine::ExactlyOne))
            .unwrap();
        composition
            .register_port(PortSpec::new(port("private_socket"), Combine::ExactlyOne))
            .unwrap();
        composition
            .register_provider(provide("socket", &["private_socket"]))
            .unwrap();
        composition
            .register_provider(
                provide("local", &["process_spawner"]).require(port("private_socket")),
            )
            .unwrap();

        let assembly = composition.resolve([port("process_spawner")]).unwrap();
        assert_eq!(
            assembly.initialization_order(),
            &[provider("socket"), provider("local")]
        );
        assert_eq!(
            assembly.port(&port("process_spawner")).unwrap().providers,
            vec![provider("local")]
        );
    }

    #[test]
    fn exactly_one_rejects_missing_and_ambiguous_provider() {
        let mut missing = Composition::new();
        missing
            .register_port(PortSpec::new(port("store"), Combine::ExactlyOne))
            .unwrap();
        assert_eq!(
            missing.resolve([port("store")]),
            Err(CompositionError::MissingProvider(port("store")))
        );

        let mut ambiguous = Composition::new();
        ambiguous
            .register_port(PortSpec::new(port("store"), Combine::ExactlyOne))
            .unwrap();
        ambiguous
            .register_provider(provide("sqlite", &["store"]))
            .unwrap();
        ambiguous
            .register_provider(provide("memory", &["store"]))
            .unwrap();
        assert!(matches!(
            ambiguous.resolve([port("store")]),
            Err(CompositionError::AmbiguousProvider { .. })
        ));
    }

    #[test]
    fn orders_chain_deterministically_and_initializes_in_the_same_order() {
        let mut composition = Composition::new();
        composition
            .register_port(PortSpec::new(port("spawn_transform"), Combine::Chain))
            .unwrap();
        composition
            .register_provider(provide("workspace", &["spawn_transform"]))
            .unwrap();
        composition
            .register_provider(
                provide("wrapper", &["spawn_transform"])
                    .after(port("spawn_transform"), provider("workspace")),
            )
            .unwrap();

        let assembly = composition.resolve([port("spawn_transform")]).unwrap();
        let expected = vec![provider("workspace"), provider("wrapper")];
        assert_eq!(
            assembly.port(&port("spawn_transform")).unwrap().providers,
            expected
        );
        assert_eq!(assembly.initialization_order(), expected);
    }

    #[test]
    fn empty_multi_ports_are_valid() {
        for combine in [Combine::Chain, Combine::All, Combine::Fanout] {
            let mut composition = Composition::new();
            composition
                .register_port(PortSpec::new(port("optional_many"), combine))
                .unwrap();
            let assembly = composition.resolve([port("optional_many")]).unwrap();
            assert!(
                assembly
                    .port(&port("optional_many"))
                    .unwrap()
                    .providers
                    .is_empty()
            );
        }
    }

    #[test]
    fn rejects_dependency_cycle() {
        let mut composition = Composition::new();
        composition
            .register_port(PortSpec::new(port("a"), Combine::ExactlyOne))
            .unwrap();
        composition
            .register_port(PortSpec::new(port("b"), Combine::ExactlyOne))
            .unwrap();
        composition
            .register_provider(provide("a-provider", &["a"]).require(port("b")))
            .unwrap();
        composition
            .register_provider(provide("b-provider", &["b"]).require(port("a")))
            .unwrap();

        assert!(matches!(
            composition.resolve([port("a")]),
            Err(CompositionError::DependencyCycle { .. })
        ));
    }

    #[test]
    fn rejects_order_cycle_and_cross_port_ordering() {
        let mut cycle = Composition::new();
        cycle
            .register_port(PortSpec::new(port("guard"), Combine::All))
            .unwrap();
        cycle
            .register_provider(provide("a", &["guard"]).before(port("guard"), provider("b")))
            .unwrap();
        cycle
            .register_provider(provide("b", &["guard"]).before(port("guard"), provider("a")))
            .unwrap();
        assert!(matches!(
            cycle.resolve([port("guard")]),
            Err(CompositionError::OrderingCycle { .. })
        ));

        let mut cross_port = Composition::new();
        cross_port
            .register_port(PortSpec::new(port("guard"), Combine::All))
            .unwrap();
        cross_port
            .register_port(PortSpec::new(port("observer"), Combine::Fanout))
            .unwrap();
        cross_port
            .register_provider(
                provide("guard", &["guard"]).before(port("guard"), provider("observer")),
            )
            .unwrap();
        cross_port
            .register_provider(provide("observer", &["observer"]))
            .unwrap();
        assert!(matches!(
            cross_port.resolve([port("guard")]),
            Err(CompositionError::InvalidOrderingTarget { .. })
        ));
    }

    #[test]
    fn manifest_contains_only_selected_providers() {
        let mut composition = Composition::new();
        composition
            .register_port(PortSpec::new(port("store"), Combine::ExactlyOne))
            .unwrap();
        composition
            .register_port(PortSpec::new(port("unused"), Combine::ExactlyOne))
            .unwrap();
        composition
            .register_provider(provide("sqlite", &["store"]))
            .unwrap();
        composition
            .register_provider(provide("unused", &["unused"]))
            .unwrap();

        let manifest = composition.resolve([port("store")]).unwrap().manifest();
        assert_eq!(manifest.providers.len(), 1);
        assert_eq!(manifest.providers[0].id, provider("sqlite"));
    }
    #[test]
    fn ordering_is_local_to_each_port_contribution() {
        let mut composition = Composition::new();
        for name in ["guard", "observer"] {
            composition
                .register_port(PortSpec::new(port(name), Combine::Chain))
                .unwrap();
        }
        composition
            .register_provider(
                provide("a", &["guard", "observer"])
                    .before(port("guard"), provider("b"))
                    .after(port("observer"), provider("b")),
            )
            .unwrap();
        composition
            .register_provider(provide("b", &["guard", "observer"]))
            .unwrap();
        composition
            .register_provider(provide("c", &["observer"]))
            .unwrap();
        let assembly = composition
            .resolve([port("guard"), port("observer")])
            .unwrap();
        assert_eq!(
            assembly.port(&port("guard")).unwrap().providers,
            vec![provider("a"), provider("b")]
        );
        assert_eq!(
            assembly.port(&port("observer")).unwrap().providers,
            vec![provider("b"), provider("a"), provider("c")]
        );
    }

    #[test]
    fn ordering_does_not_require_target_to_contribute_unrelated_ports() {
        let mut composition = Composition::new();
        for name in ["guard", "observer"] {
            composition
                .register_port(PortSpec::new(port(name), Combine::Chain))
                .unwrap();
        }
        composition
            .register_provider(
                provide("a", &["guard", "observer"]).after(port("guard"), provider("b")),
            )
            .unwrap();
        composition
            .register_provider(provide("b", &["guard"]))
            .unwrap();
        let assembly = composition
            .resolve([port("guard"), port("observer")])
            .unwrap();
        assert_eq!(
            assembly.port(&port("guard")).unwrap().providers,
            vec![provider("b"), provider("a")]
        );
        assert_eq!(
            assembly.port(&port("observer")).unwrap().providers,
            vec![provider("a")]
        );
    }
}
