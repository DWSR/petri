//! Run-owned dispatch to linked or external sandbox providers.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use executor::{
    AcquireContext, EnvError, EnvHandle, Executor, ReleaseReport, Retention, SandboxLeaseId,
    ScopeOutcome, ScopeSpec, WorkspaceId,
};
use ir::RuntimeTarget;
use sandbox_driver::SandboxId;
use tokio::fs;
use tokio::sync::OnceCell;

use crate::actions::{ActionHost, ActionHostRunner, remove_recorded};
use crate::gate::{DRAIN_BUDGET, RunGate};
use crate::in_process::{InProcessProviders, InProcessSource, ProviderContext};
use crate::lease::{LeaseLedger, MemoryLedger, ReconcileReport, RecordedLease};
use crate::plugin::{FixedProvider, PluginSource, ProviderSource};
use crate::registration::{ProviderRegistrations, ProviderTransport, is_builtin};
use crate::run::{RunIdentity, workspace_dir};
use crate::simulated::{SIMULATED_KIND, SimulatedProvider};
use crate::{SandboxBackend, SandboxExecutor, SandboxOptions, SandboxTeardown};

pub const CONTAINER_KIND: &str = "docker";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ActionOwner {
    Lease(SandboxLeaseId),
    Scope(ir::ScopeId),
}

type ProviderSlot = Arc<OnceCell<Result<Arc<SandboxExecutor>, String>>>;

/// One lazy executor per provider kind, shared by execution and recovery.
/// Registrations are configuration only; an unused provider never connects.
/// Recorded leases always resolve through their owning provider kind.
pub struct RoutingExecutor {
    providers:    ProviderRegistrations,
    slots:        Mutex<BTreeMap<String, ProviderSlot>>,
    fixed:        Option<Arc<dyn ProviderSource>>,
    action_hosts: Mutex<BTreeMap<(ActionOwner, WorkspaceId), Arc<ActionHost>>>,
    ledger:       OnceLock<Arc<dyn LeaseLedger>>,
    identity:     Arc<RunIdentity>,
    retention:    Retention,
    options:      SandboxOptions,
    simulated:    bool,
    gate:         RunGate,
}

impl RoutingExecutor {
    pub fn local(run_dir: impl Into<PathBuf>, retention: Retention) -> Self {
        Self::with_options(run_dir, retention, SandboxOptions::default())
    }

    pub fn local_with_dev(run_dir: impl Into<PathBuf>, retention: Retention, dev: bool) -> Self {
        Self::with_options(run_dir, retention, SandboxOptions {
            plugin_dev: Some(dev),
            ..Default::default()
        })
    }

    pub fn with_options(
        run_dir: impl Into<PathBuf>,
        retention: Retention,
        options: SandboxOptions,
    ) -> Self {
        Self {
            providers: ProviderRegistrations::from_env(options.plugin_dev),
            slots: Mutex::new(BTreeMap::new()),
            fixed: None,
            action_hosts: Mutex::new(BTreeMap::new()),
            ledger: OnceLock::new(),
            identity: Arc::new(RunIdentity::for_run_dir(run_dir.into())),
            retention,
            options,
            simulated: false,
            gate: RunGate::default(),
        }
    }

    /// A caller-owned source for container scopes, primarily for tests.
    pub fn with_provider_source(
        source: Arc<dyn ProviderSource>,
        run_dir: impl Into<PathBuf>,
        retention: Retention,
    ) -> Self {
        let mut router = Self::local(run_dir, retention);
        router.fixed = Some(source);
        router
    }

    pub fn with_provider(
        provider: Arc<dyn sandbox_driver::SandboxProvider>,
        run_dir: impl Into<PathBuf>,
        retention: Retention,
    ) -> Self {
        Self::with_provider_source(Arc::new(FixedProvider::new(provider)), run_dir, retention)
    }

    /// A dry run never resolves or contacts a configured provider.
    pub fn simulated(run_dir: impl Into<PathBuf>) -> Self {
        let source = Arc::new(FixedProvider::new(Arc::new(SimulatedProvider::new())));
        let mut router = Self::with_provider_source(source, run_dir, Retention::Never);
        router.simulated = true;
        router
    }

    /// Compatibility adapter: a closed registry containing only these
    /// factories.
    #[must_use]
    pub fn with_in_process(self, providers: InProcessProviders) -> Self {
        self.with_providers(providers.into())
    }

    /// Install the complete registry. Missing entries never use ambient
    /// fallback.
    #[must_use]
    pub fn with_providers(mut self, providers: ProviderRegistrations) -> Self {
        self.providers = providers;
        self
    }

    #[must_use]
    pub fn with_run_id(mut self, run_id: impl Into<smol_str::SmolStr>) -> Self {
        self.identity = Arc::new(RunIdentity::new(
            self.identity.run_dir().to_path_buf(),
            run_id,
        ));
        self
    }

    pub fn set_ledger(&self, ledger: Arc<dyn LeaseLedger>) {
        let _ = self.ledger.set(ledger);
    }

    pub fn identity(&self) -> &Arc<RunIdentity> {
        &self.identity
    }

    pub async fn shutdown(&self) {
        let executors: Vec<_> = self
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter_map(|slot| slot.get().and_then(|result| result.as_ref().ok()).cloned())
            .collect();
        let settles = executors
            .iter()
            .any(|executor| !executor.manager().source().ends_work_on_shutdown());
        let budget = if settles {
            DRAIN_BUDGET
        } else {
            Duration::ZERO
        };
        if !self.gate.close(budget).await && !budget.is_zero() {
            tracing::warn!(
                "sandbox operations were still running when the run closed its sandboxes"
            );
        }
        // A configuration that completed while the gate closed belongs to
        // this shutdown too. Acquires check the gate before using a source.
        let executors: Vec<_> = self
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter_map(|slot| slot.get().and_then(|result| result.as_ref().ok()).cloned())
            .collect();
        for executor in executors {
            let source = executor.manager().source();
            if self
                .fixed
                .as_ref()
                .is_none_or(|fixed| !Arc::ptr_eq(fixed, source))
            {
                source.shutdown().await;
            }
        }
    }

    /// Placement chooses a kind; the registry chooses its transport.
    pub fn provider_kind_for(&self, runtime: &ir::RuntimeSpec) -> &str {
        if self.simulated {
            return SIMULATED_KIND;
        }
        if self.options.backend == SandboxBackend::Host
            && matches!(runtime.target, RuntimeTarget::HostProcess)
        {
            return "host";
        }
        if let Some(source) = &self.fixed {
            return source.kind();
        }
        match &self.options.backend {
            SandboxBackend::Host | SandboxBackend::Docker => "docker",
            SandboxBackend::Daytona => "daytona",
            SandboxBackend::Registered(kind) => kind.as_str(),
        }
    }

    async fn executor_for_kind(&self, kind: &str) -> Result<Arc<SandboxExecutor>, EnvError> {
        if self.simulated && kind != SIMULATED_KIND {
            return Err(EnvError::backend(
                kind,
                "resolve",
                "a dry run leaves real provider resources untouched",
            ));
        }
        let slot = self
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(kind.to_owned())
            .or_default()
            .clone();
        slot.get_or_init(|| self.build_executor(kind))
            .await
            .clone()
            .map_err(|message| EnvError::backend(kind, "configure", message))
    }

    async fn build_executor(&self, kind: &str) -> Result<Arc<SandboxExecutor>, String> {
        let ledger = self
            .ledger
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::new(MemoryLedger::default()));
        if let Some(source) = self.fixed.as_ref().filter(|source| source.kind() == kind) {
            let executor = if self.simulated {
                SandboxExecutor::simulated(source.clone(), ledger, self.identity.clone())
            } else {
                SandboxExecutor::new(
                    source.clone(),
                    ledger,
                    self.identity.clone(),
                    self.retention,
                    Some(crate::DOCKER_HOST_ALIAS.to_owned()),
                    self.options.clone(),
                )
                .with_host_workspace(true)
            };
            return Ok(Arc::new(executor.with_gate(self.gate.clone())));
        }
        let registration = self.providers.get(kind)?;
        let host_registry = if kind == "host" {
            let directory = self.identity.run_dir().join("host-registry");
            fs::create_dir_all(&directory)
                .await
                .map_err(|error| error.to_string())?;
            Some(
                fs::canonicalize(directory)
                    .await
                    .map_err(|error| error.to_string())?,
            )
        } else {
            None
        };
        let source: Arc<dyn ProviderSource> = match &registration.transport {
            ProviderTransport::Linked(factory) => {
                let context = ProviderContext::new(
                    self.identity.run_id(),
                    self.identity.run_dir(),
                    host_registry,
                );
                let mut source = InProcessSource::new(factory.clone(), context);
                if !is_builtin(kind) {
                    source = source.with_namespace(registration.namespace.clone());
                }
                Arc::new(source)
            }
            ProviderTransport::Plugin(settings) => {
                let mut settings = settings.clone();
                if let Some(directory) = &host_registry {
                    settings = settings.with_host_registry(directory);
                }
                if !is_builtin(kind) {
                    settings = settings.with_namespace(registration.namespace.clone());
                }
                Arc::new(PluginSource::new(settings))
            }
        };
        let mut options = self.options.clone();
        options.backend = kind.parse::<SandboxBackend>()?;
        let mut executor = SandboxExecutor::new(
            registration.mask_source(source),
            ledger,
            self.identity.clone(),
            self.retention,
            registration.network.host_address()?,
            options,
        )
        .with_gate(self.gate.clone())
        .with_host_workspace(registration.network.supports_host_workspace());
        if let Some(spec) = &registration.process_spec {
            executor = executor.with_process_spec(spec.clone(), registration.labels.clone());
        } else if !is_builtin(kind) {
            return Err(format!(
                "sandbox provider `{kind}` needs a process specification"
            ));
        }
        Ok(Arc::new(executor))
    }

    async fn recorded_provider(&self, lease: SandboxLeaseId) -> Result<Option<String>, EnvError> {
        let Some(ledger) = self.ledger.get() else {
            return Ok(None);
        };
        let record = ledger
            .lookup(lease)
            .await
            .map_err(|error| EnvError::backend("sandbox", "lookup", error.to_string()))?;
        Ok(record.and_then(|record| record.provider.map(|kind| kind.to_string())))
    }

    async fn executor_for_record(
        &self,
        lease: SandboxLeaseId,
    ) -> Result<Arc<SandboxExecutor>, EnvError> {
        let kind = self.recorded_provider(lease).await?.ok_or_else(|| {
            EnvError::backend(
                "sandbox",
                "lookup",
                format!("lease {lease} has no recorded provider"),
            )
        })?;
        self.executor_for_kind(&kind).await
    }

    /// The name prefix every sandbox this run owns starts with,
    /// `petri-<run id>-`, for a leak check after release.
    pub fn container_prefix(&self) -> String {
        self.identity.container_prefix()
    }

    async fn action_host(
        &self,
        owner: ActionOwner,
        scope: &ScopeSpec,
    ) -> Result<Arc<ActionHost>, EnvError> {
        let provider = self.executor_for_kind("docker").await?;
        if !provider.supports_host_workspace {
            return Err(EnvError::backend(
                CONTAINER_KIND,
                "one-shot",
                "Docker actions in Host jobs require a local Docker daemon to mount their workspace; \
                 use --backend docker for a remote daemon",
            ));
        }
        let host_address = provider
            .host_address
            .as_ref()
            .ok_or(EnvError::HostUnreachable)?;
        let key = (owner, scope.workspace_id.clone());
        let mut hosts = self
            .action_hosts
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Ok(hosts
            .entry(key)
            .or_insert_with(|| {
                Arc::new(ActionHost::new(
                    provider.manager().source().clone(),
                    self.identity.clone(),
                    workspace_dir(self.identity.run_dir(), scope.workspace_id.as_str()),
                    scope.workspace_id.as_str().to_owned(),
                    host_address.clone(),
                ))
            })
            .clone())
    }

    /// Keep failed hosts registered so a later release can retry cleanup.
    async fn release_action_hosts(&self, owner: ActionOwner) -> ReleaseReport {
        let hosts: Vec<_> = self
            .action_hosts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|((candidate, _), _)| *candidate == owner)
            .map(|(key, host)| (key.clone(), host.clone()))
            .collect();
        let mut report = ReleaseReport::default();
        for (key, host) in hosts {
            match host.teardown().await {
                Ok(removed) => {
                    if removed {
                        report = report.released("action host");
                    }
                    self.action_hosts
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .remove(&key);
                }
                Err(error) => {
                    report = report.problem(format!("action host teardown failed: {error}"));
                }
            }
        }
        report
    }

    /// Ends a lease: stops its sandbox, then keeps or deletes it by this
    /// router's retention for `outcome`. The coordinator calls this when
    /// the invocation that owns the lease finishes.
    pub async fn release_lease(
        &self,
        lease: SandboxLeaseId,
        outcome: ScopeOutcome,
    ) -> ReleaseReport {
        let mut report = self.release_action_hosts(ActionOwner::Lease(lease)).await;
        if !report.is_clean() {
            // The action host can still have this workspace mounted. Keep its
            // owning lease intact so release or prune can retry safely.
            return report;
        }
        match self.executor_for_record(lease).await {
            Ok(container) => {
                let ended = container
                    .manager()
                    .release_lease(lease, self.retention, outcome)
                    .await;
                report.merge(ended);
            }
            Err(error) => {
                report = report.problem(format!(
                    "no provider executor to release lease {lease}: {error}"
                ));
            }
        }
        report
    }

    /// Reconcile each recorded provider independently. The two slices are
    /// retained for callers of the former Host/container API; the ledger's
    /// provider identity determines every group.
    pub async fn reconcile(
        &self,
        host: &[RecordedLease],
        container: &[RecordedLease],
    ) -> ReconcileReport {
        let mut report = ReconcileReport::default();
        let mut groups: BTreeMap<String, Vec<RecordedLease>> = BTreeMap::new();
        for lease in host.iter().chain(container) {
            match self.recorded_provider(lease.lease).await {
                Ok(Some(kind)) => groups.entry(kind).or_default().push(lease.clone()),
                Ok(None) => report
                    .problems
                    .push(format!("lease {} has no recorded provider", lease.lease)),
                Err(error) => report.problems.push(error.to_string()),
            }
        }
        for (kind, leases) in groups {
            match self.executor_for_kind(&kind).await {
                Ok(executor) => report.merge(executor.manager().reconcile(&leases).await),
                Err(error) => report.problems.push(error.to_string()),
            }
        }
        report
    }

    /// Deletes a recorded lease's sandbox with no live handle: prune.
    /// `workspace_id` is the lease's workspace, the reconcile key for a
    /// record that never learned its resource id.
    pub async fn delete_recorded(
        &self,
        lease: SandboxLeaseId,
        workspace_id: &str,
    ) -> Result<Vec<SandboxId>, EnvError> {
        if !self.simulated
            && self.recorded_provider(lease).await?.as_deref() == Some(SIMULATED_KIND)
        {
            // A dry run's lease: nothing was ever on a provider.
            return Ok(Vec::new());
        }
        let executor = self.executor_for_record(lease).await?;
        // An action host has its own marker and Docker resource. Remove it
        // before the Host provider deletes the workspace, even if an earlier
        // cleanup left a tombstone for the Host lease itself.
        // The container provider is only needed when a marker says one
        // created an action host.
        let mut deleted = if executor.manager().source().kind() == "host" {
            let docker = self.executor_for_kind("docker").await;
            let source = match &docker {
                Ok(executor) => Ok(&**executor.manager().source()),
                Err(error) => Err(EnvError::backend("docker", "configure", error.to_string())),
            };
            remove_recorded(source, &self.identity, workspace_id, None).await?
        } else {
            Vec::new()
        };
        deleted.extend(
            executor
                .manager()
                .delete_recorded(lease, workspace_id)
                .await?,
        );
        Ok(deleted)
    }

    /// Check every namespace in each provider group before prune starts
    /// deleting. Independent matching groups can still be pruned.
    pub async fn verify_recorded(
        &self,
        leases: &[RecordedLease],
    ) -> BTreeMap<SandboxLeaseId, String> {
        let mut problems = BTreeMap::new();
        let mut groups: BTreeMap<String, Vec<RecordedLease>> = BTreeMap::new();
        let Some(ledger) = self.ledger.get() else {
            return problems;
        };
        for lease in leases {
            match ledger.lookup(lease.lease).await {
                Ok(Some(record))
                    if record.state == crate::LeaseState::Deleted
                        || record.provider.as_deref() == Some(SIMULATED_KIND)
                        || (record.state == crate::LeaseState::Allocating
                            && record.pending.is_none()
                            && record.fingerprint.is_none()
                            && record.resource_id.is_none()) => {}
                Ok(Some(record)) => match record.provider {
                    Some(kind) => groups
                        .entry(kind.to_string())
                        .or_default()
                        .push(lease.clone()),
                    None => {
                        problems.insert(lease.lease, "lease has no recorded provider".to_owned());
                    }
                },
                Ok(None) => {
                    problems.insert(lease.lease, "lease is missing from the ledger".to_owned());
                }
                Err(error) => {
                    problems.insert(lease.lease, error.to_string());
                }
            }
        }
        for (kind, group) in groups {
            let result = match self.executor_for_kind(&kind).await {
                Ok(executor) => executor.manager().verify_namespace(&group).await,
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                for lease in group {
                    problems.insert(lease.lease, error.to_string());
                }
            }
        }
        problems
    }
}

#[async_trait]
impl Executor for RoutingExecutor {
    async fn acquire(
        &self,
        scope: &ScopeSpec,
        ctx: &AcquireContext,
    ) -> Result<EnvHandle, EnvError> {
        let _admission = self.gate.admit("acquire")?;
        let selected = self.provider_kind_for(&scope.runtime);
        let recorded = if let (Some(ledger), Some(lease)) = (self.ledger.get(), ctx.lease()) {
            ledger
                .lookup(lease)
                .await
                .map_err(|error| EnvError::backend("sandbox", "lookup", error.to_string()))?
        } else {
            None
        };
        let kind = match recorded
            .as_ref()
            .and_then(|record| record.provider.as_deref())
        {
            Some(kind) if !self.simulated => {
                if kind != selected
                    && recorded
                        .as_ref()
                        .is_some_and(|record| record.fingerprint.is_none())
                {
                    return Err(EnvError::backend(
                        kind,
                        "acquire",
                        "an existing reservation cannot change provider",
                    ));
                }
                kind
            }
            _ => selected,
        };
        let executor = self.executor_for_kind(kind).await?;
        if kind == "host" && matches!(scope.runtime.target, RuntimeTarget::HostProcess) {
            let owner = ctx
                .lease()
                .map_or(ActionOwner::Scope(scope.id), ActionOwner::Lease);
            let action_host = self.action_host(owner, scope).await;
            if let Ok(host) = &action_host {
                host.prepare().await;
            }
            let handle = executor.acquire(scope, ctx).await?;
            Ok(handle.with_runner(Arc::new(ActionHostRunner::new(
                action_host,
                scope,
                self.gate.clone(),
            ))))
        } else {
            executor.acquire(scope, ctx).await
        }
    }

    /// Release goes back to the backend whose teardown record the handle
    /// carries: the handle itself says which acquired it.
    async fn release(&self, env: EnvHandle, outcome: ScopeOutcome) -> ReleaseReport {
        let Some(teardown) = env.teardown::<SandboxTeardown>() else {
            return ReleaseReport::default()
                .problem("no backend of this router acquired this environment");
        };
        let executor = match self.executor_for_kind(&teardown.provider).await {
            Ok(executor) => executor,
            Err(error) => return ReleaseReport::default().problem(error.to_string()),
        };
        let ended = self
            .release_action_hosts(ActionOwner::Scope(env.scope()))
            .await;
        if !ended.is_clean() {
            // End the holder but retain its workspace while an action host
            // can still mount it. A later acquire/release can retry cleanup.
            executor.manager().release_holder(teardown.lease).await;
            return ended;
        }
        let mut report = executor.release(env, outcome).await;
        report.merge(ended);
        report
    }
}
