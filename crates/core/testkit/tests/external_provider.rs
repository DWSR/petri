//! Unfamiliar providers through the real stdio/socket protocol, without Docker.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use executor::{
    AcquireContext, Executor as _, ProcessSpec, Retention, SandboxLeaseId, ScopeOutcome, ScopeSpec,
};
use executor_sandbox::{
    LeaseLedger as _, LeaseState, MemoryLedger, PluginSettings, PluginSource, ProviderRegistration,
    ProviderRegistrations, RecordedLease, RoutingExecutor, SandboxOptions,
};
use ir::ScopeId;
use sandbox_driver::{ProviderKind, SandboxSource, SandboxSpec, WorkspaceOwnership};
use sandbox_driver_protocol::PluginConfig;
use testkit::RunDir;
use testkit::in_process::{HostFactory, UnusedFactory};

const LEASE: SandboxLeaseId = SandboxLeaseId::new(7);

fn registration(dir: &RunDir, identity: Option<&str>) -> ProviderRegistration {
    named_registration(dir, identity, "fixture-provider")
}

fn named_registration(dir: &RunDir, identity: Option<&str>, kind: &str) -> ProviderRegistration {
    let config = plugin_config(dir, identity, kind);
    let mut spec = SandboxSpec::new(SandboxSource::HostDirectory)
        .working_directory(dir.path().join("workspace").to_string_lossy())
        .env_var("TEMPLATE_ENV", "template-value")
        .label("custom-label", "preserved")
        .provider_config(serde_json::json!({"opaque": "preserved"}));
    spec.workspace_ownership = Some(WorkspaceOwnership::Managed);
    ProviderRegistration::plugin(PluginSettings::try_new(config).expect("valid explicit config"))
        .process_spec(spec)
}

fn plugin_config(dir: &RunDir, identity: Option<&str>, kind: &str) -> PluginConfig {
    let mut config = PluginConfig::new(ProviderKind::try_new(kind).expect("valid fixture kind"));
    config.path = Some(env!("CARGO_BIN_EXE_fixture-provider").into());
    config.args = vec!["--fixture".to_owned()];
    config.dev = true;
    config
        .env
        .insert("FIXTURE_KIND".to_owned(), kind.to_owned());
    config.env.insert(
        "FIXTURE_ROOT".to_owned(),
        dir.path().join("backend").to_string_lossy().into_owned(),
    );
    config
        .env
        .insert("PATH".to_owned(), "/usr/bin:/bin".to_owned());
    if let Some(identity) = identity {
        config
            .env
            .insert("FIXTURE_IDENTITY".to_owned(), identity.to_owned());
    }
    config
}

#[tokio::test]
async fn an_invalid_checksum_or_declared_kind_never_serves_a_registration() {
    let dir = RunDir::new("external-launch-refusals");
    let mut config = plugin_config(&dir, Some("account-a"), "fixture-provider");
    config.sha256 = Some("0".repeat(64));
    let source = PluginSource::new(PluginSettings::try_new(config).unwrap());
    let error = source.current().await.err().expect("checksum refuses");
    assert!(error.to_string().contains("checksum mismatch"), "{error}");
    assert!(calls(&dir).is_empty(), "bad checksum never launches");
    source.shutdown().await;

    let mut config = plugin_config(&dir, Some("account-a"), "fixture-provider");
    config
        .env
        .insert("FIXTURE_KIND".to_owned(), "different-kind".to_owned());
    let source = PluginSource::new(PluginSettings::try_new(config).unwrap());
    let error = source.current().await.err().expect("kind refuses");
    assert!(
        error.to_string().contains("different provider kind"),
        "{error}"
    );
    source.shutdown().await;
    assert!(
        calls(&dir)
            .lines()
            .all(|call| matches!(call, "launch" | "health"))
    );
}

#[tokio::test]
async fn an_unregistered_kind_never_falls_back_to_host() {
    let dir = RunDir::new("external-missing-registration");
    let router = RoutingExecutor::with_options(dir.path(), Retention::Never, SandboxOptions {
        backend: "unregistered-kind".parse().unwrap(),
        ..Default::default()
    });
    let error = router
        .acquire(&scope(), &AcquireContext::bare())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not registered"), "{error}");
    assert!(!dir.path().join("host-registry").exists());
    router.shutdown().await;
}

#[tokio::test]
async fn one_registry_routes_two_custom_providers_and_a_linked_host_by_recorded_kind() {
    let run = RunDir::new("external-mixed-run");
    let alpha = RunDir::new("external-alpha");
    let beta = RunDir::new("external-beta");
    let ledger = Arc::new(MemoryLedger::default());
    let host = Arc::new(HostFactory::default());
    let docker = Arc::new(UnusedFactory::new("docker"));
    let mut providers = ProviderRegistrations::new();
    providers
        .register(ProviderRegistration::linked(host.clone()))
        .unwrap();
    providers
        .register(ProviderRegistration::linked(docker.clone()))
        .unwrap();
    providers
        .register(named_registration(&alpha, Some("a"), "provider-alpha"))
        .unwrap();
    providers
        .register(named_registration(&beta, Some("b"), "provider-beta"))
        .unwrap();
    let make_router = |selected: &str| {
        let router = RoutingExecutor::with_options(run.path(), Retention::Always, SandboxOptions {
            backend: selected.parse().expect("valid kind"),
            ..Default::default()
        })
        .with_providers(providers.clone());
        router.set_ledger(ledger.clone());
        router
    };
    for (lease, kind) in [(1, "provider-alpha"), (2, "provider-beta")] {
        let router = make_router(kind);
        let mut scope = scope();
        scope.workspace_id = executor::WorkspaceId::new(format!("work-{lease}"));
        let env = router
            .acquire(
                &scope,
                &AcquireContext::bare().with_lease(SandboxLeaseId::new(lease)),
            )
            .await
            .unwrap();
        env.exec()
            .write_file(Path::new("owner"), kind.as_bytes())
            .await
            .unwrap();
        router.release(env, ScopeOutcome::Succeeded).await;
        router.shutdown().await;
    }
    let router = make_router("host");
    let recorded = [1, 2].map(|lease| RecordedLease {
        lease:        SandboxLeaseId::new(lease),
        workspace_id: format!("work-{lease}"),
    });
    assert!(router.reconcile(&[], &recorded).await.problems.is_empty());
    for (lease, kind) in [(1, "provider-alpha"), (2, "provider-beta")] {
        let mut scope = scope();
        scope.workspace_id = executor::WorkspaceId::new(format!("work-{lease}"));
        let env = router
            .acquire(
                &scope,
                &AcquireContext::bare().with_lease(SandboxLeaseId::new(lease)),
            )
            .await
            .unwrap();
        assert_eq!(
            env.exec()
                .read_file(Path::new("owner"))
                .await
                .unwrap()
                .unwrap(),
            kind.as_bytes()
        );
        router.release(env, ScopeOutcome::Succeeded).await;
        router
            .delete_recorded(SandboxLeaseId::new(lease), scope.workspace_id.as_str())
            .await
            .unwrap();
    }
    let host_env = router
        .acquire(
            &scope(),
            &AcquireContext::bare().with_lease(SandboxLeaseId::new(3)),
        )
        .await
        .unwrap();
    host_env
        .exec()
        .write_file(Path::new("host"), b"linked")
        .await
        .unwrap();
    router.release(host_env, ScopeOutcome::Succeeded).await;
    router
        .delete_recorded(SandboxLeaseId::new(3), "work")
        .await
        .unwrap();
    router.shutdown().await;
    assert_eq!(host.connects(), 1);
    assert_eq!(docker.connects(), 0);
    for dir in [&alpha, &beta] {
        assert_eq!(
            calls(dir).lines().filter(|call| *call == "create").count(),
            1
        );
    }
}

fn router(
    dir: &RunDir,
    ledger: &Arc<MemoryLedger>,
    selected: &str,
    identity: Option<&str>,
) -> RoutingExecutor {
    let mut providers = ProviderRegistrations::new();
    providers
        .register(ProviderRegistration::linked(Arc::new(
            HostFactory::default(),
        )))
        .expect("valid fixture state");
    providers
        .register(ProviderRegistration::linked(Arc::new(UnusedFactory::new(
            "docker",
        ))))
        .expect("valid fixture state");
    providers
        .register(registration(dir, identity))
        .expect("valid fixture state");
    let router = RoutingExecutor::with_options(dir.path(), Retention::Always, SandboxOptions {
        backend: selected.parse().expect("valid fixture state"),
        ..Default::default()
    })
    .with_providers(providers);
    router.set_ledger(ledger.clone());
    router
}

fn scope() -> ScopeSpec {
    let mut scope = ScopeSpec::new(ScopeId::new(0), "work");
    scope.env.insert("SCOPE_ENV".into(), "scope-value".into());
    scope
}

fn recorded() -> Vec<RecordedLease> {
    vec![RecordedLease {
        lease:        LEASE,
        workspace_id: "work".to_owned(),
    }]
}

fn calls(dir: &RunDir) -> String {
    fs::read_to_string(dir.path().join("backend/calls")).unwrap_or_default()
}

#[tokio::test]
async fn plugin_exec_files_and_recovery_use_the_recorded_provider_after_default_changes() {
    let dir = RunDir::new("external-recovery");
    let ledger = Arc::new(MemoryLedger::default());
    let first = router(&dir, &ledger, "fixture-provider", Some("account-a"));
    assert!(calls(&dir).is_empty(), "registration is lazy");
    let env = first
        .acquire(&scope(), &AcquireContext::bare().with_lease(LEASE))
        .await
        .expect("valid fixture state");
    let exec = env.exec();
    exec.write_file(Path::new("input"), b"persistent")
        .await
        .expect("valid fixture state");
    let mut child = exec
        .spawn(ProcessSpec::new("/bin/sh", &[
            "-c",
            "printf '%s:%s:' \"$TEMPLATE_ENV\" \"$SCOPE_ENV\" > output; cat input >> output",
        ]))
        .await
        .expect("valid fixture state");
    assert_eq!(
        child.wait().await.expect("valid fixture state").code,
        Some(0)
    );
    assert_eq!(
        exec.read_file(Path::new("output"))
            .await
            .expect("valid fixture state")
            .expect("valid fixture state"),
        b"template-value:scope-value:persistent"
    );
    let spec: serde_json::Value = serde_json::from_slice(
        &fs::read(dir.path().join("backend/spec.json")).expect("valid fixture state"),
    )
    .expect("valid fixture state");
    assert_eq!(spec["labels"]["custom-label"], "preserved");
    assert_eq!(spec["provider_config"]["opaque"], "preserved");
    assert!(
        spec["name"]
            .as_str()
            .expect("valid fixture state")
            .starts_with("petri-")
    );
    assert!(first.release(env, ScopeOutcome::Succeeded).await.is_clean());
    first.shutdown().await;
    assert!(
        exec.read_file(Path::new("input")).await.is_err(),
        "old generation is closed"
    );

    let second = router(&dir, &ledger, "host", Some("account-a"));
    let env = second
        .acquire(&scope(), &AcquireContext::bare().with_lease(LEASE))
        .await
        .expect("valid fixture state");
    assert_eq!(
        env.exec()
            .read_file(Path::new("input"))
            .await
            .expect("valid fixture state")
            .expect("valid fixture state"),
        b"persistent"
    );
    assert!(
        second
            .release(env, ScopeOutcome::Succeeded)
            .await
            .is_clean()
    );
    let report = second.release_lease(LEASE, ScopeOutcome::Succeeded).await;
    assert!(report.is_clean(), "{report:?}");
    assert_eq!(
        ledger
            .lookup(LEASE)
            .await
            .expect("valid fixture state")
            .expect("valid fixture state")
            .state,
        LeaseState::Stopped
    );
    assert!(second.verify_recorded(&recorded()).await.is_empty());
    assert_eq!(
        second
            .delete_recorded(LEASE, "work")
            .await
            .expect("valid fixture state")
            .len(),
        1
    );
    assert_eq!(
        ledger
            .lookup(LEASE)
            .await
            .expect("valid fixture state")
            .expect("valid fixture state")
            .state,
        LeaseState::Deleted
    );
    second.shutdown().await;
    assert_eq!(
        calls(&dir).lines().filter(|call| *call == "create").count(),
        1
    );
    assert_eq!(
        calls(&dir).lines().filter(|call| *call == "launch").count(),
        2
    );
}

#[tokio::test]
async fn namespace_mismatch_refuses_attach_create_delete_and_orphan_sweeping() {
    let dir = RunDir::new("external-namespace");
    let ledger = Arc::new(MemoryLedger::default());
    let first = router(&dir, &ledger, "fixture-provider", Some("account-a"));
    let env = first
        .acquire(&scope(), &AcquireContext::bare().with_lease(LEASE))
        .await
        .expect("valid fixture state");
    first.release(env, ScopeOutcome::Succeeded).await;
    first.shutdown().await;
    let before = calls(&dir);
    let second = router(&dir, &ledger, "fixture-provider", Some("account-b"));
    assert!(!second.verify_recorded(&recorded()).await.is_empty());
    let report = second.reconcile(&[], &recorded()).await;
    assert!(!report.problems.is_empty());
    assert!(
        second
            .acquire(
                &scope(),
                &AcquireContext::bare().with_lease(SandboxLeaseId::new(8))
            )
            .await
            .is_err()
    );
    assert!(second.delete_recorded(LEASE, "work").await.is_err());
    second.shutdown().await;
    let after = calls(&dir);
    for call in after
        .strip_prefix(&before)
        .expect("valid fixture state")
        .lines()
    {
        assert!(
            matches!(call, "launch" | "health"),
            "unexpected backend operation: {call}"
        );
    }
    assert_eq!(
        ledger
            .lookup(LEASE)
            .await
            .expect("valid fixture state")
            .expect("valid fixture state")
            .state,
        LeaseState::Live
    );
    let cleanup = router(&dir, &ledger, "host", Some("account-a"));
    cleanup
        .delete_recorded(LEASE, "work")
        .await
        .expect("valid fixture state");
    cleanup.shutdown().await;
}

#[tokio::test]
async fn lost_create_reply_is_reconciled_without_replaying_the_mutation() {
    let dir = RunDir::new("external-crash");
    fs::create_dir_all(dir.path().join("backend")).expect("valid fixture state");
    fs::write(dir.path().join("backend/crash-after-create"), "once").expect("valid fixture state");
    let ledger = Arc::new(MemoryLedger::default());
    let router = router(&dir, &ledger, "fixture-provider", Some("account-a"));
    assert!(
        router
            .acquire(&scope(), &AcquireContext::bare().with_lease(LEASE))
            .await
            .is_err()
    );
    assert_eq!(
        ledger
            .lookup(LEASE)
            .await
            .expect("valid fixture state")
            .expect("valid fixture state")
            .state,
        LeaseState::Allocating
    );
    let env = router
        .acquire(&scope(), &AcquireContext::bare().with_lease(LEASE))
        .await
        .expect("valid fixture state");
    env.exec()
        .write_file(Path::new("recovered"), b"yes")
        .await
        .expect("valid fixture state");
    router.release(env, ScopeOutcome::Succeeded).await;
    router
        .delete_recorded(LEASE, "work")
        .await
        .expect("valid fixture state");
    router.shutdown().await;
    assert_eq!(
        calls(&dir).lines().filter(|call| *call == "create").count(),
        1
    );
    assert_eq!(
        calls(&dir).lines().filter(|call| *call == "launch").count(),
        2
    );
}

#[tokio::test]
async fn missing_identity_and_unsupported_container_scope_never_create() {
    let dir = RunDir::new("external-refusals");
    let ledger = Arc::new(MemoryLedger::default());
    let router = router(&dir, &ledger, "fixture-provider", None);
    let mut container = scope();
    container.runtime = ir::RuntimeSpec::container("unused");
    let error = router
        .acquire(&container, &AcquireContext::bare())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("process scopes"), "{error}");
    assert!(calls(&dir).is_empty(), "reject placement before launching");
    assert!(
        router
            .acquire(&scope(), &AcquireContext::bare())
            .await
            .is_err()
    );
    assert!(!calls(&dir).lines().any(|call| call == "create"));
    router.shutdown().await;
}
