use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use cue_core::vnext::{ExecutionProjection, FactDraft, FactEvent, OutputStream, Pipeline, Scope};
use cue_core::{EventId, ExecutionId, ScopeHash, StepId};
use thiserror::Error;

use crate::{Assembly, AssemblyManifest, ProviderId, RuntimePort};

pub type RuntimeFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeErrorKind {
    InvalidInput,
    NotFound,
    Conflict,
    Unsupported,
    Infrastructure,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{kind:?}: {message}")]
pub struct RuntimeError {
    pub kind: RuntimeErrorKind,
    pub message: String,
}

impl RuntimeError {
    pub fn new(kind: RuntimeErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn infrastructure(message: impl Into<String>) -> Self {
        Self::new(RuntimeErrorKind::Infrastructure, message)
    }
}

pub trait ExecutionStore: Send + Sync {
    fn get(&self, id: ExecutionId) -> Result<Option<ExecutionProjection>, RuntimeError>;
    fn list(
        &self,
        before: Option<ExecutionId>,
        limit: u16,
    ) -> Result<Vec<ExecutionProjection>, RuntimeError>;
    fn commit(
        &self,
        execution: &ExecutionProjection,
        facts: &[FactDraft],
    ) -> Result<Vec<FactEvent>, RuntimeError>;
    fn facts_after(
        &self,
        execution: ExecutionId,
        after: Option<EventId>,
        limit: u16,
    ) -> Result<Vec<FactEvent>, RuntimeError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeDurability {
    Durable,
    Volatile,
}

pub trait ScopeStore: Send + Sync {
    fn put(&self, scope: &Scope, created_at_ms: i64) -> Result<ScopeDurability, RuntimeError>;
    fn get(&self, hash: ScopeHash) -> Result<Option<Scope>, RuntimeError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputSlice {
    pub offset: u64,
    pub data: Vec<u8>,
    pub next_offset: u64,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputAppend {
    pub start_offset: u64,
    pub end_offset: u64,
}

pub trait OutputStore: Send + Sync {
    fn append(
        &self,
        step: StepId,
        stream: OutputStream,
        data: &[u8],
    ) -> Result<OutputAppend, RuntimeError>;
    fn read(
        &self,
        step: StepId,
        stream: OutputStream,
        offset: u64,
        maximum: usize,
    ) -> Result<OutputSlice, RuntimeError>;
    fn tail(
        &self,
        step: StepId,
        stream: OutputStream,
        maximum: usize,
    ) -> Result<OutputSlice, RuntimeError>;
}

#[derive(Debug, Clone)]
pub struct SpawnRequest {
    pub step: StepId,
    pub pipeline: Pipeline,
    pub io: cue_core::vnext::IoMode,
    pub scope: Scope,
}

pub trait ProcessSpawner: Send + Sync {
    fn spawn(&self, request: SpawnRequest) -> RuntimeFuture<Result<SpawnedRun, RuntimeError>>;
}

pub trait Workspace: Send + Sync {
    fn materialize(&self, request: &mut SpawnRequest) -> Result<(), RuntimeError>;
}

pub trait SpawnTransform: Send + Sync {
    fn transform(&self, request: &mut SpawnRequest) -> Result<(), RuntimeError>;
}

pub trait SpawnGuard: Send + Sync {
    fn check(&self, request: &SpawnRequest) -> Result<(), RuntimeError>;
}

pub trait ExecutionObserver: Send + Sync {
    fn observe(&self, fact: &FactEvent) -> Result<(), RuntimeError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunExit {
    Success,
    ExitCode(i32),
    Signalled,
    Cancelled,
    SpawnFailed(String),
    InfrastructureFailure(String),
}

pub struct SpawnedRun {
    pub control: RunControl,
    completion: tokio::sync::oneshot::Receiver<RunExit>,
}

impl SpawnedRun {
    pub(crate) fn new(
        control: RunControl,
        completion: tokio::sync::oneshot::Receiver<RunExit>,
    ) -> Self {
        Self {
            control,
            completion,
        }
    }

    pub async fn wait(self) -> RunExit {
        self.completion.await.unwrap_or_else(|_| {
            RunExit::InfrastructureFailure("runner completion channel closed".into())
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalSize {
    pub columns: u16,
    pub rows: u16,
}

impl TerminalSize {
    pub fn new(columns: u16, rows: u16) -> Result<Self, RuntimeError> {
        if columns == 0 || rows == 0 {
            return Err(RuntimeError::new(
                RuntimeErrorKind::InvalidInput,
                "terminal dimensions must be non-zero",
            ));
        }
        Ok(Self { columns, rows })
    }
}

#[derive(Clone)]
pub struct RunControl {
    pub(crate) mode: cue_core::vnext::IoMode,
    pub(crate) sender: tokio::sync::mpsc::Sender<RunControlCommand>,
}

impl RunControl {
    pub async fn terminate(&self, mode: cue_core::vnext::CancelMode) -> Result<(), RuntimeError> {
        self.request(RunControlRequest::Terminate(mode)).await
    }

    pub async fn input(&self, data: Vec<u8>) -> Result<(), RuntimeError> {
        if self.mode != cue_core::vnext::IoMode::Pty {
            return Err(RuntimeError::new(
                RuntimeErrorKind::Unsupported,
                "captured runs do not accept terminal input",
            ));
        }
        self.request(RunControlRequest::Input(data)).await
    }

    pub async fn resize(&self, size: TerminalSize) -> Result<(), RuntimeError> {
        if self.mode != cue_core::vnext::IoMode::Pty {
            return Err(RuntimeError::new(
                RuntimeErrorKind::Unsupported,
                "captured runs do not have a terminal size",
            ));
        }
        self.request(RunControlRequest::Resize(size)).await
    }

    async fn request(&self, request: RunControlRequest) -> Result<(), RuntimeError> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sender
            .send(RunControlCommand { request, reply })
            .await
            .map_err(|_| {
                RuntimeError::new(RuntimeErrorKind::Conflict, "run is no longer active")
            })?;
        receive
            .await
            .map_err(|_| RuntimeError::infrastructure("runner dropped a control acknowledgement"))?
    }
}

pub(crate) enum RunControlRequest {
    Terminate(cue_core::vnext::CancelMode),
    Input(Vec<u8>),
    Resize(TerminalSize),
}

pub(crate) struct RunControlCommand {
    pub request: RunControlRequest,
    pub reply: tokio::sync::oneshot::Sender<Result<(), RuntimeError>>,
}

#[derive(Default)]
pub struct ProviderBundle {
    pub execution_store: Option<Arc<dyn ExecutionStore>>,
    pub scope_store: Option<Arc<dyn ScopeStore>>,
    pub output_store: Option<Arc<dyn OutputStore>>,
    pub process_spawner: Option<Arc<dyn ProcessSpawner>>,
    pub workspace: Option<Arc<dyn Workspace>>,
    pub spawn_transform: Option<Arc<dyn SpawnTransform>>,
    pub spawn_guard: Option<Arc<dyn SpawnGuard>>,
    pub execution_observer: Option<Arc<dyn ExecutionObserver>>,
}

#[derive(Default)]
pub struct ProviderRegistry {
    providers: BTreeMap<ProviderId, ProviderBundle>,
}

impl ProviderRegistry {
    pub fn insert(
        &mut self,
        id: ProviderId,
        provider: ProviderBundle,
    ) -> Result<(), AssemblyBindingError> {
        if self.providers.insert(id.clone(), provider).is_some() {
            return Err(AssemblyBindingError::DuplicateProvider(id));
        }
        Ok(())
    }
}

pub struct RuntimeAssembly {
    execution_store: Arc<dyn ExecutionStore>,
    scope_store: Arc<dyn ScopeStore>,
    output_store: Arc<dyn OutputStore>,
    process_spawner: Arc<dyn ProcessSpawner>,
    workspace: Option<Arc<dyn Workspace>>,
    spawn_transforms: Vec<Arc<dyn SpawnTransform>>,
    spawn_guards: Vec<Arc<dyn SpawnGuard>>,
    observers: Vec<Arc<dyn ExecutionObserver>>,
    manifest: AssemblyManifest,
}

impl RuntimeAssembly {
    pub fn bind(
        assembly: Assembly,
        registry: ProviderRegistry,
    ) -> Result<Self, AssemblyBindingError> {
        let execution_store = bind_exact(
            &assembly,
            &registry,
            RuntimePort::ExecutionStore,
            |provider| provider.execution_store.clone(),
        )?;
        let scope_store = bind_exact(&assembly, &registry, RuntimePort::ScopeStore, |provider| {
            provider.scope_store.clone()
        })?;
        let output_store =
            bind_exact(&assembly, &registry, RuntimePort::OutputStore, |provider| {
                provider.output_store.clone()
            })?;
        let process_spawner = bind_exact(
            &assembly,
            &registry,
            RuntimePort::ProcessSpawner,
            |provider| provider.process_spawner.clone(),
        )?;
        let workspace = bind_optional(&assembly, &registry, RuntimePort::Workspace, |provider| {
            provider.workspace.clone()
        })?;
        let spawn_transforms = bind_many(
            &assembly,
            &registry,
            RuntimePort::SpawnTransform,
            |provider| provider.spawn_transform.clone(),
        )?;
        let spawn_guards = bind_many(&assembly, &registry, RuntimePort::SpawnGuard, |provider| {
            provider.spawn_guard.clone()
        })?;
        let observers = bind_many(
            &assembly,
            &registry,
            RuntimePort::ExecutionObserver,
            |provider| provider.execution_observer.clone(),
        )?;
        let manifest = assembly.manifest();
        Ok(Self {
            execution_store,
            scope_store,
            output_store,
            process_spawner,
            workspace,
            spawn_transforms,
            spawn_guards,
            observers,
            manifest,
        })
    }

    pub fn execution_store(&self) -> &Arc<dyn ExecutionStore> {
        &self.execution_store
    }

    pub fn scope_store(&self) -> &Arc<dyn ScopeStore> {
        &self.scope_store
    }

    pub fn output_store(&self) -> &Arc<dyn OutputStore> {
        &self.output_store
    }

    pub fn manifest(&self) -> &AssemblyManifest {
        &self.manifest
    }

    pub fn prepare_spawn(&self, mut request: SpawnRequest) -> Result<SpawnRequest, RuntimeError> {
        if let Some(workspace) = &self.workspace {
            workspace.materialize(&mut request)?;
        }
        for transform in &self.spawn_transforms {
            transform.transform(&mut request)?;
        }
        for guard in &self.spawn_guards {
            guard.check(&request)?;
        }
        Ok(request)
    }

    pub fn spawn(&self, request: SpawnRequest) -> RuntimeFuture<Result<SpawnedRun, RuntimeError>> {
        match self.prepare_spawn(request) {
            Ok(request) => self.process_spawner.spawn(request),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    pub fn observe(&self, fact: &FactEvent) -> Result<(), RuntimeError> {
        for observer in &self.observers {
            observer.observe(fact)?;
        }
        Ok(())
    }
}

fn selected(assembly: &Assembly, port: RuntimePort) -> Result<&[ProviderId], AssemblyBindingError> {
    Ok(&assembly
        .port(&port.port_id())
        .ok_or(AssemblyBindingError::MissingResolvedPort(port))?
        .providers)
}

fn bundle<'a>(
    registry: &'a ProviderRegistry,
    provider: &ProviderId,
    port: RuntimePort,
) -> Result<&'a ProviderBundle, AssemblyBindingError> {
    registry
        .providers
        .get(provider)
        .ok_or_else(|| AssemblyBindingError::MissingProvider {
            provider: provider.clone(),
            port,
        })
}

fn bind_exact<T: ?Sized>(
    assembly: &Assembly,
    registry: &ProviderRegistry,
    port: RuntimePort,
    implementation: impl Fn(&ProviderBundle) -> Option<Arc<T>>,
) -> Result<Arc<T>, AssemblyBindingError> {
    let provider = selected(assembly, port)?
        .first()
        .ok_or(AssemblyBindingError::MissingResolvedProvider(port))?;
    implementation(bundle(registry, provider, port)?).ok_or_else(|| {
        AssemblyBindingError::MissingImplementation {
            provider: provider.clone(),
            port,
        }
    })
}

fn bind_optional<T: ?Sized>(
    assembly: &Assembly,
    registry: &ProviderRegistry,
    port: RuntimePort,
    implementation: impl Fn(&ProviderBundle) -> Option<Arc<T>>,
) -> Result<Option<Arc<T>>, AssemblyBindingError> {
    selected(assembly, port)?
        .first()
        .map(|provider| {
            implementation(bundle(registry, provider, port)?).ok_or_else(|| {
                AssemblyBindingError::MissingImplementation {
                    provider: provider.clone(),
                    port,
                }
            })
        })
        .transpose()
}

fn bind_many<T: ?Sized>(
    assembly: &Assembly,
    registry: &ProviderRegistry,
    port: RuntimePort,
    implementation: impl Fn(&ProviderBundle) -> Option<Arc<T>>,
) -> Result<Vec<Arc<T>>, AssemblyBindingError> {
    selected(assembly, port)?
        .iter()
        .map(|provider| {
            implementation(bundle(registry, provider, port)?).ok_or_else(|| {
                AssemblyBindingError::MissingImplementation {
                    provider: provider.clone(),
                    port,
                }
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AssemblyBindingError {
    #[error("provider {0} was registered more than once")]
    DuplicateProvider(ProviderId),
    #[error("runtime Assembly did not resolve canonical port {0:?}")]
    MissingResolvedPort(RuntimePort),
    #[error("runtime Assembly resolved no provider for required port {0:?}")]
    MissingResolvedProvider(RuntimePort),
    #[error("resolved provider {provider} for {port:?} has no runtime instance")]
    MissingProvider {
        provider: ProviderId,
        port: RuntimePort,
    },
    #[error("provider {provider} does not implement resolved port {port:?}")]
    MissingImplementation {
        provider: ProviderId,
        port: RuntimePort,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use cue_core::vnext::{AbsolutePath, Argv, FileModeMask, IoMode, Pipeline, Process};

    use super::*;
    use crate::{Composition, ProviderSpec, canonical_port_specs, runtime_root_ports};

    struct NullExecutionStore;

    impl ExecutionStore for NullExecutionStore {
        fn get(&self, _id: ExecutionId) -> Result<Option<ExecutionProjection>, RuntimeError> {
            Ok(None)
        }

        fn list(
            &self,
            _before: Option<ExecutionId>,
            _limit: u16,
        ) -> Result<Vec<ExecutionProjection>, RuntimeError> {
            Ok(Vec::new())
        }

        fn commit(
            &self,
            _execution: &ExecutionProjection,
            _facts: &[FactDraft],
        ) -> Result<Vec<FactEvent>, RuntimeError> {
            Ok(Vec::new())
        }

        fn facts_after(
            &self,
            _execution: ExecutionId,
            _after: Option<EventId>,
            _limit: u16,
        ) -> Result<Vec<FactEvent>, RuntimeError> {
            Ok(Vec::new())
        }
    }

    struct NullScopeStore;

    impl ScopeStore for NullScopeStore {
        fn put(
            &self,
            _scope: &Scope,
            _created_at_ms: i64,
        ) -> Result<ScopeDurability, RuntimeError> {
            Ok(ScopeDurability::Durable)
        }

        fn get(&self, _hash: ScopeHash) -> Result<Option<Scope>, RuntimeError> {
            Ok(None)
        }
    }

    struct NullSpawner;

    impl ProcessSpawner for NullSpawner {
        fn spawn(&self, _request: SpawnRequest) -> RuntimeFuture<Result<SpawnedRun, RuntimeError>> {
            Box::pin(async { Err(RuntimeError::infrastructure("not called")) })
        }
    }

    struct RecordingTransform {
        name: &'static str,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl SpawnTransform for RecordingTransform {
        fn transform(&self, _request: &mut SpawnRequest) -> Result<(), RuntimeError> {
            self.calls.lock().unwrap().push(self.name);
            Ok(())
        }
    }

    fn provider(value: &str) -> ProviderId {
        ProviderId::new(value).unwrap()
    }

    fn resolved() -> Assembly {
        let mut composition = Composition::new();
        for port in canonical_port_specs() {
            composition.register_port(port).unwrap();
        }
        composition
            .register_provider(
                ProviderSpec::new(
                    provider("base"),
                    "1",
                    [
                        RuntimePort::ExecutionStore.port_id(),
                        RuntimePort::ScopeStore.port_id(),
                        RuntimePort::OutputStore.port_id(),
                        RuntimePort::ProcessSpawner.port_id(),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        composition
            .register_provider(
                ProviderSpec::new(
                    provider("first"),
                    "1",
                    [RuntimePort::SpawnTransform.port_id()],
                )
                .unwrap()
                .before(provider("second")),
            )
            .unwrap();
        composition
            .register_provider(
                ProviderSpec::new(
                    provider("second"),
                    "1",
                    [RuntimePort::SpawnTransform.port_id()],
                )
                .unwrap(),
            )
            .unwrap();
        composition.resolve(runtime_root_ports()).unwrap()
    }

    fn request() -> SpawnRequest {
        let scope = Scope::new(
            AbsolutePath::new("/workspace").unwrap(),
            BTreeMap::new(),
            FileModeMask::new(0o022).unwrap(),
        );
        let pipeline = Pipeline::simple(Process::new(Argv::new("true", Vec::new()).unwrap()));
        SpawnRequest {
            step: StepId {
                execution: ExecutionId(1),
                index: 1,
            },
            pipeline,
            io: IoMode::Captured,
            scope,
        }
    }

    #[test]
    fn binding_consumes_metadata_into_typed_fields_and_ordered_chains() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ProviderRegistry::default();
        registry
            .insert(
                provider("base"),
                ProviderBundle {
                    execution_store: Some(Arc::new(NullExecutionStore)),
                    scope_store: Some(Arc::new(NullScopeStore)),
                    output_store: Some(Arc::new(crate::MemoryOutputStore::default())),
                    process_spawner: Some(Arc::new(NullSpawner)),
                    ..ProviderBundle::default()
                },
            )
            .unwrap();
        for name in ["first", "second"] {
            registry
                .insert(
                    provider(name),
                    ProviderBundle {
                        spawn_transform: Some(Arc::new(RecordingTransform {
                            name: if name == "first" { "first" } else { "second" },
                            calls: calls.clone(),
                        })),
                        ..ProviderBundle::default()
                    },
                )
                .unwrap();
        }

        let runtime = RuntimeAssembly::bind(resolved(), registry).unwrap();
        runtime.prepare_spawn(request()).unwrap();
        assert_eq!(*calls.lock().unwrap(), vec!["first", "second"]);
        assert_eq!(runtime.manifest().providers.len(), 3);
    }

    #[test]
    fn binding_rejects_metadata_without_the_typed_implementation() {
        let mut registry = ProviderRegistry::default();
        registry
            .insert(
                provider("base"),
                ProviderBundle {
                    execution_store: Some(Arc::new(NullExecutionStore)),
                    scope_store: Some(Arc::new(NullScopeStore)),
                    output_store: Some(Arc::new(crate::MemoryOutputStore::default())),
                    ..ProviderBundle::default()
                },
            )
            .unwrap();
        assert!(matches!(
            RuntimeAssembly::bind(resolved(), registry),
            Err(AssemblyBindingError::MissingImplementation {
                port: RuntimePort::ProcessSpawner,
                ..
            })
        ));
    }

    #[test]
    fn binding_rejects_an_assembly_that_did_not_resolve_every_runtime_root() {
        let mut composition = Composition::new();
        composition
            .register_port(RuntimePort::ExecutionStore.specification())
            .unwrap();
        composition
            .register_provider(
                ProviderSpec::new(
                    provider("base"),
                    "1",
                    [RuntimePort::ExecutionStore.port_id()],
                )
                .unwrap(),
            )
            .unwrap();
        let assembly = composition
            .resolve([RuntimePort::ExecutionStore.port_id()])
            .unwrap();
        let mut registry = ProviderRegistry::default();
        registry
            .insert(
                provider("base"),
                ProviderBundle {
                    execution_store: Some(Arc::new(NullExecutionStore)),
                    ..ProviderBundle::default()
                },
            )
            .unwrap();
        assert!(matches!(
            RuntimeAssembly::bind(assembly, registry),
            Err(AssemblyBindingError::MissingResolvedPort(
                RuntimePort::ScopeStore
            ))
        ));
    }
}
