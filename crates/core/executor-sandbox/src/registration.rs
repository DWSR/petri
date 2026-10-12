//! Provider bindings shared by execution, recovery, and pruning.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use executor::{EnvError, Masker};
use sandbox_driver::{ProviderKind, SandboxProvider, SandboxSpec};

use crate::plugin::{ProviderSource, mask_error};
use crate::{InProcessProviders, PluginSettings, ProviderFactory, ProviderNetwork};

/// Why a provider registration cannot be used. Configuration values are
/// deliberately omitted: plugin arguments, environment, and specs may be
/// secret.
#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    #[error("invalid provider kind; use lowercase letters, digits, and interior hyphens")]
    InvalidKind,
    #[error("`local` and `simulated` are reserved provider names")]
    ReservedKind,
    #[error("provider `{0}` is already registered; use replace for an explicit override")]
    Duplicate(String),
    #[error("provider `{0}` needs a process sandbox specification")]
    MissingProcessSpec(String),
    #[error("invalid process sandbox specification: {0}")]
    InvalidSpec(&'static str),
    #[error("invalid plugin launch configuration: {0}")]
    InvalidLaunch(&'static str),
    #[error("a configured resource namespace must be nonempty")]
    InvalidNamespace,
}

/// Evidence identifying the custom provider's resource namespace.
/// Values must be non-secret and stable across credential rotation.
#[derive(Clone, Debug)]
pub enum NamespacePolicy {
    /// Require a nonempty identity from the provider's health report.
    HealthIdentity,
    /// An operator-supplied namespace, combined with health identity when
    /// present.
    Configured(String),
    /// Require health identity as well as this configured namespace.
    Both(String),
}

impl NamespacePolicy {
    pub(crate) fn validate(&self) -> Result<(), RegistrationError> {
        if matches!(self, Self::Configured(value) | Self::Both(value) if value.trim().is_empty()) {
            return Err(RegistrationError::InvalidNamespace);
        }
        Ok(())
    }

    pub(crate) fn configured(&self) -> Option<&str> {
        match self {
            Self::Configured(value) | Self::Both(value) => Some(value),
            Self::HealthIdentity => None,
        }
    }
}

#[derive(Clone)]
pub(crate) enum ProviderTransport {
    Linked(Arc<dyn ProviderFactory>),
    Plugin(PluginSettings),
}

/// One provider's connection and process-scope preparation. Constructing a
/// registration never launches a plugin or contacts a provider.
#[derive(Clone)]
pub struct ProviderRegistration {
    pub(crate) transport:    ProviderTransport,
    pub(crate) network:      ProviderNetwork,
    pub(crate) process_spec: Option<SandboxSpec>,
    pub(crate) namespace:    NamespacePolicy,
    pub(crate) labels:       BTreeSet<String>,
}

impl fmt::Debug for ProviderRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderRegistration")
            .field("kind", &self.kind())
            .field(
                "plugin",
                &matches!(self.transport, ProviderTransport::Plugin(_)),
            )
            .field("process_spec", &self.process_spec.is_some())
            .finish_non_exhaustive()
    }
}

impl ProviderRegistration {
    pub(crate) fn mask_source(&self, source: Arc<dyn ProviderSource>) -> Arc<dyn ProviderSource> {
        let masker = Masker::new();
        self.register_masks(&masker);
        Arc::new(RegisteredSource { source, masker })
    }

    fn register_masks(&self, masker: &Masker) {
        if let ProviderTransport::Plugin(settings) = &self.transport {
            settings.register_masks(masker);
        }
        if let Some(spec) = &self.process_spec {
            for value in spec.env.values() {
                masker.register_explicit(value);
            }
            mask_json_strings(masker, &spec.provider_config);
        }
    }
    pub fn linked(factory: Arc<dyn ProviderFactory>) -> Self {
        Self {
            network:      factory.network(),
            transport:    ProviderTransport::Linked(factory),
            process_spec: None,
            namespace:    NamespacePolicy::HealthIdentity,
            labels:       BTreeSet::new(),
        }
    }

    pub fn plugin(settings: PluginSettings) -> Self {
        Self {
            network:      settings.network(),
            transport:    ProviderTransport::Plugin(settings),
            process_spec: None,
            namespace:    NamespacePolicy::HealthIdentity,
            labels:       BTreeSet::new(),
        }
    }

    /// The existing SandboxSpec contract; Petri supplies names and reserved
    /// labels and overlays each scope's environment. Validated at registration.
    #[must_use]
    pub fn process_spec(mut self, spec: SandboxSpec) -> Self {
        self.process_spec = Some(spec);
        self
    }

    #[must_use]
    pub fn namespace(mut self, namespace: NamespacePolicy) -> Self {
        self.namespace = namespace;
        self
    }

    #[must_use]
    pub fn network(mut self, network: ProviderNetwork) -> Self {
        self.network = network;
        self
    }

    /// Workflow placement labels this particular template fulfills.
    #[must_use]
    pub fn placement_labels(mut self, labels: BTreeSet<String>) -> Self {
        self.labels = labels;
        self
    }

    pub fn kind(&self) -> &str {
        match &self.transport {
            ProviderTransport::Linked(factory) => factory.kind(),
            ProviderTransport::Plugin(settings) => settings.kind().as_str(),
        }
    }

    fn validate(&self) -> Result<(), RegistrationError> {
        validate_kind(self.kind())?;
        self.namespace.validate()?;
        if is_builtin(self.kind()) {
            if !matches!(self.namespace, NamespacePolicy::HealthIdentity) {
                return Err(RegistrationError::InvalidSpec(
                    "built-in providers use their existing namespace fingerprints",
                ));
            }
            if self.process_spec.is_some() || !self.labels.is_empty() {
                return Err(RegistrationError::InvalidSpec(
                    "built-in providers keep their preparation recipe",
                ));
            }
        } else {
            let spec = self
                .process_spec
                .as_ref()
                .ok_or_else(|| RegistrationError::MissingProcessSpec(self.kind().to_owned()))?;
            if spec.name.is_some() || spec.labels.keys().any(|key| key.starts_with("petri.")) {
                return Err(RegistrationError::InvalidSpec(
                    "Petri owns the resource name and petri.* labels",
                ));
            }
            spec.network
                .validate()
                .map_err(|_| RegistrationError::InvalidSpec("invalid network policy"))?;
        }
        Ok(())
    }
}

pub(crate) fn is_builtin(kind: &str) -> bool {
    matches!(kind, "host" | "docker" | "daytona")
}

pub(crate) fn validate_kind(kind: &str) -> Result<ProviderKind, RegistrationError> {
    if matches!(kind, "local" | "simulated") {
        return Err(RegistrationError::ReservedKind);
    }
    ProviderKind::try_new(kind).map_err(|_| RegistrationError::InvalidKind)
}

/// A closed set of configured providers, mixing linked factories and plugins.
/// Missing entries never fall back to ambient executable discovery.
#[derive(Clone, Debug, Default)]
pub struct ProviderRegistrations {
    entries: BTreeMap<String, ProviderRegistration>,
}

impl ProviderRegistrations {
    pub fn new() -> Self {
        Self::default()
    }

    /// Include registration values in the host's existing output mask set.
    pub fn register_masks(&self, masker: &executor::Masker) {
        for registration in self.entries.values() {
            registration.register_masks(masker);
        }
    }

    /// Standalone Petri's first-party plugin defaults. This only snapshots
    /// configuration; executable resolution and health checks remain lazy.
    pub fn from_env(dev: Option<bool>) -> Self {
        let entries = ["host", "docker", "daytona"]
            .into_iter()
            .map(|kind| {
                let settings =
                    PluginSettings::from_env(kind, dev).expect("built-in kinds are valid");
                (kind.to_owned(), ProviderRegistration::plugin(settings))
            })
            .collect();
        Self { entries }
    }

    pub fn register(
        &mut self,
        registration: ProviderRegistration,
    ) -> Result<(), RegistrationError> {
        registration.validate()?;
        if self.entries.contains_key(registration.kind()) {
            return Err(RegistrationError::Duplicate(registration.kind().to_owned()));
        }
        self.entries
            .insert(registration.kind().to_owned(), registration);
        Ok(())
    }

    /// Explicitly replace a binding, or add it if absent.
    pub fn replace(&mut self, registration: ProviderRegistration) -> Result<(), RegistrationError> {
        registration.validate()?;
        self.entries
            .insert(registration.kind().to_owned(), registration);
        Ok(())
    }

    pub(crate) fn get(&self, kind: &str) -> Result<&ProviderRegistration, String> {
        self.entries
            .get(kind)
            .ok_or_else(|| format!("sandbox provider `{kind}` is not registered"))
    }
}

/// Keep registration data out of errors regardless of the transport used.
struct RegisteredSource {
    source: Arc<dyn ProviderSource>,
    masker: Masker,
}

#[async_trait::async_trait]
impl ProviderSource for RegisteredSource {
    async fn current(&self) -> Result<(Arc<dyn SandboxProvider>, u64), EnvError> {
        self.source
            .current()
            .await
            .map_err(|error| mask_error(self, error))
    }
    async fn shutdown(&self) {
        self.source.shutdown().await;
    }
    fn ends_work_on_shutdown(&self) -> bool {
        self.source.ends_work_on_shutdown()
    }
    fn fingerprint(&self) -> &str {
        self.source.fingerprint()
    }
    fn kind(&self) -> &str {
        self.source.kind()
    }
    fn region(&self) -> Option<&str> {
        self.source.region()
    }
    fn masker(&self) -> Masker {
        self.masker.clone()
    }
}

fn mask_json_strings(masker: &executor::Masker, value: &serde_json::Value) {
    match value {
        serde_json::Value::String(value) => masker.register_explicit(value),
        serde_json::Value::Array(values) => {
            for value in values {
                mask_json_strings(masker, value);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                mask_json_strings(masker, value);
            }
        }
        _ => {}
    }
}

impl From<InProcessProviders> for ProviderRegistrations {
    fn from(providers: InProcessProviders) -> Self {
        // Preserve the legacy builder's infallible API and closed lookup.
        // Only its established built-in placement recipes are usable.
        Self {
            entries: providers
                .into_factories()
                .into_iter()
                .map(|(kind, factory)| (kind, ProviderRegistration::linked(factory)))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use sandbox_driver::{HealthStatus, ProviderHealth, SandboxSource};
    use sandbox_driver_protocol::PluginConfig;

    use super::*;
    use crate::fingerprint;

    fn config(kind: &str) -> PluginConfig {
        let mut config = PluginConfig::new(ProviderKind::try_new(kind).expect("valid kind"));
        config.path = Some("/not-installed/provider".into());
        config.dev = true;
        config
    }

    fn registration(kind: &str) -> ProviderRegistration {
        ProviderRegistration::plugin(PluginSettings::try_new(config(kind)).expect("valid launch"))
            .process_spec(SandboxSpec::new(SandboxSource::HostDirectory))
    }

    #[tokio::test]
    async fn closing_an_unused_source_never_launches_it_later() {
        let source =
            crate::PluginSource::new(PluginSettings::try_new(config("custom-kind")).unwrap());
        source.shutdown().await;
        let error = source
            .current()
            .await
            .err()
            .expect("a closed source refuses");
        assert!(error.to_string().contains("finished"));
    }

    #[test]
    fn kinds_duplicates_templates_and_explicit_replacement_are_checked_without_launching() {
        let mut providers = ProviderRegistrations::new();
        providers.register(registration("custom-kind")).unwrap();
        assert!(matches!(
            providers.register(registration("custom-kind")),
            Err(RegistrationError::Duplicate(_))
        ));
        providers.replace(registration("custom-kind")).unwrap();
        assert!(providers.get("missing").is_err());
        assert!(PluginSettings::try_new(config("simulated")).is_err());
        assert!(PluginSettings::try_new(config("local")).is_err());
        let missing_spec =
            ProviderRegistration::plugin(PluginSettings::try_new(config("another-kind")).unwrap());
        assert!(matches!(
            providers.register(missing_spec),
            Err(RegistrationError::MissingProcessSpec(_))
        ));
        let named = registration("named-kind")
            .process_spec(SandboxSpec::new(SandboxSource::HostDirectory).name("reserved"));
        assert!(providers.register(named).is_err());
        let labeled = registration("labeled-kind").process_spec(
            SandboxSpec::new(SandboxSource::HostDirectory).label("petri.run", "reserved"),
        );
        assert!(providers.register(labeled).is_err());
        assert!(
            providers.register(registration("host")).is_err(),
            "built-in recipe cannot be replaced by a generic template"
        );
        assert!(
            providers
                .register(
                    registration("empty-namespace")
                        .namespace(NamespacePolicy::Configured(" ".to_owned()))
                )
                .is_err()
        );
    }

    #[test]
    fn explicit_launch_requires_a_path_and_trust_and_rejects_ambient_inheritance() {
        let mut launch = config("custom-kind");
        launch.dev = false;
        assert!(PluginSettings::try_new(launch.clone()).is_err());
        launch.sha256 = Some("not-a-checksum".to_owned());
        assert!(PluginSettings::try_new(launch.clone()).is_err());
        launch.sha256 = Some("a".repeat(64));
        assert!(PluginSettings::try_new(launch.clone()).is_ok());
        launch.inherit_env.push("SECRET".to_owned());
        assert!(PluginSettings::try_new(launch.clone()).is_err());
        launch.inherit_env.clear();
        launch.path = Some("provider-on-path".into());
        assert!(PluginSettings::try_new(launch.clone()).is_err());
        launch.path = None;
        assert!(PluginSettings::try_new(launch).is_err());
    }

    #[test]
    fn custom_fingerprints_require_namespace_evidence_and_separate_each_component() {
        let mut health = ProviderHealth::new(HealthStatus::Ok);
        assert!(
            fingerprint::custom("custom-kind", &NamespacePolicy::HealthIdentity, &health).is_err()
        );
        let namespace = NamespacePolicy::Configured("account-a".to_owned());
        let configured = fingerprint::custom("custom-kind", &namespace, &health).unwrap();
        assert!(!configured.contains("account-a"));
        health.identity = Some("region-a".to_owned());
        let both = fingerprint::custom("custom-kind", &namespace, &health).unwrap();
        assert_ne!(configured, both);
        assert_ne!(
            both,
            fingerprint::custom("other-kind", &namespace, &health).unwrap()
        );
        health.identity = Some("region-b".to_owned());
        assert_ne!(
            both,
            fingerprint::custom("custom-kind", &namespace, &health).unwrap()
        );
    }

    #[test]
    fn debug_and_masks_do_not_expose_launch_values_or_template_secrets() {
        let mut launch = config("custom-kind");
        launch
            .env
            .insert("TOKEN".to_owned(), "env-secret".to_owned());
        launch.args.push("arg-secret".to_owned());
        let settings = PluginSettings::try_new(launch).unwrap();
        assert!(!format!("{settings:?}").contains("env-secret"));
        assert!(!format!("{settings:?}").contains("arg-secret"));
        let registration = ProviderRegistration::plugin(settings).process_spec(
            SandboxSpec::new(SandboxSource::HostDirectory)
                .env_var("TOKEN", "spec-secret")
                .provider_config(serde_json::json!({"nested": ["opaque-secret"]})),
        );
        let mut providers = ProviderRegistrations::new();
        providers.register(registration).unwrap();
        let masker = executor::Masker::new();
        providers.register_masks(&masker);
        for secret in ["env-secret", "arg-secret", "spec-secret", "opaque-secret"] {
            assert!(!format!("{providers:?}").contains(secret));
            assert!(!masker.mask(secret).contains(secret));
        }
    }
}
