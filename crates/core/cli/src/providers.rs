//! Explicit, per-command provider registrations. Nothing from this file is
//! copied into the workflow graph or the run's durable records.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf, absolute};

use runtime::executor::sandbox::PluginSettings;
use runtime::{NamespacePolicy, ProviderNetwork, ProviderRegistration, Runtime};
use sandbox_driver::{ProviderKind, SandboxSpec};
use sandbox_driver_protocol::PluginConfig;
use serde::Deserialize;
use tokio::fs;
use toml::de::Error as TomlError;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderFile {
    providers: BTreeMap<String, ProviderEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderEntry {
    executable:              PathBuf,
    #[serde(default)]
    args:                    Vec<String>,
    sha256:                  Option<String>,
    #[serde(default)]
    dev:                     bool,
    #[serde(default)]
    env:                     BTreeMap<String, String>,
    namespace:               Option<String>,
    host_address:            Option<String>,
    #[serde(default)]
    supports_host_workspace: bool,
    #[serde(default)]
    placement_labels:        BTreeSet<String>,
    process_spec:            Option<toml::Value>,
}

impl ProviderEntry {
    fn registration(self, kind: &str, base: &Path) -> Result<ProviderRegistration, String> {
        if self.executable.as_os_str().is_empty() {
            return Err("provider executable path must be nonempty".to_owned());
        }
        let mut config =
            PluginConfig::new(ProviderKind::try_new(kind).map_err(|_| "invalid provider kind")?);
        let executable = if self.executable.is_absolute() {
            self.executable
        } else {
            base.join(self.executable)
        };
        config.path =
            Some(absolute(executable).map_err(|_| "cannot resolve provider executable path")?);
        config.args = self.args;
        config.env = self.env;
        config.sha256 = self.sha256;
        config.dev = self.dev;
        let settings = PluginSettings::try_new(config).map_err(|error| error.to_string())?;
        let mut registration = ProviderRegistration::plugin(settings);
        if let Some(value) = self.process_spec {
            let fields = [
                "name",
                "source",
                "resources",
                "sandbox_kind",
                "env",
                "labels",
                "user",
                "working_directory",
                "workspace_ownership",
                "network",
                "volumes",
                "timers",
                "ephemeral",
                "public",
                "region",
                "provider_config",
            ];
            if value
                .as_table()
                .is_none_or(|table| table.keys().any(|key| !fields.contains(&key.as_str())))
            {
                return Err("process_spec contains an unknown field".to_owned());
            }
            let spec: SandboxSpec = value
                .try_into()
                .map_err(|_| "invalid process_spec; use the sandbox-driver SandboxSpec schema")?;
            registration = registration.process_spec(spec);
        }
        if let Some(namespace) = self.namespace {
            registration = registration.namespace(NamespacePolicy::Configured(namespace));
        }
        if !matches!(kind, "host" | "docker" | "daytona")
            || self.host_address.is_some()
            || self.supports_host_workspace
        {
            registration = registration.network(ProviderNetwork::new(
                self.host_address,
                self.supports_host_workspace,
            ));
        }
        Ok(registration.placement_labels(self.placement_labels))
    }
}

pub(crate) async fn configure(mut runtime: Runtime, path: &Path) -> Result<Runtime, String> {
    let text = fs::read_to_string(path)
        .await
        .map_err(|error| format!("reading sandbox provider file: {error}"))?;
    // TOML diagnostics can quote credentials from the input. Keep only a
    // safe location, never the source text or the parser's source chain.
    let file: ProviderFile = toml::from_str(&text).map_err(|error: TomlError| {
        format!(
            "invalid sandbox provider TOML near byte {} (check field names and value types)",
            error.span().map_or(0, |span| span.start)
        )
    })?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    for (kind, entry) in file.providers {
        let registration = entry.registration(&kind, base)?;
        runtime = runtime
            .sandbox_provider(registration)
            .map_err(|error| error.to_string())?;
    }
    Ok(runtime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_relative_executable_is_resolved_before_registration_without_opening_it() {
        let entry: ProviderEntry = toml::from_str(
            "executable = 'not-installed-provider'\ndev = true\n[process_spec]\nsource = 'host_directory'\n",
        ).unwrap();
        assert!(entry.registration("custom-kind", Path::new(".")).is_ok());
    }
}
