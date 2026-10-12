# Registered sandbox providers

Petri can combine linked provider factories and external sandbox-driver plugins
in one runtime. The standalone CLI keeps its Host, Docker, and Daytona plugin
defaults. Embedders can keep linked defaults and replace individual kinds.

## CLI

Pass the same provider file to `run`, `resume`, and `sandbox prune`:

```sh
petri run workflow.petri.yaml --run-dir ./run --backend fixture-provider --sandbox-providers providers.toml
petri resume --run-dir ./run --sandbox-providers providers.toml
petri sandbox prune --run-dir ./run --sandbox-providers providers.toml
```

This example uses the repository's directory-backed test plugin. Build it with
`cargo build -p petri-testkit --bin fixture-provider`.

```toml
[providers.fixture-provider]
executable = "./target/debug/fixture-provider"
args = ["--fixture"]
dev = true
# Production: omit dev and set sha256 to the executable's 64-digit SHA-256.

[providers.fixture-provider.env]
PATH = "/usr/bin:/bin"
FIXTURE_ROOT = "/tmp/petri-provider-example/backend"
FIXTURE_IDENTITY = "example-account"

[providers.fixture-provider.process_spec]
source = "host_directory"
working_directory = "/tmp/petri-provider-example/workspace"
workspace_ownership = "managed"
```

Executable paths are relative to the provider file. Explicit registrations do
not search `PATH` or inherit Petri's environment. Put required values in `env`.
The plugin runs on Petri's host and speaks sandbox-driver protocol v2:
JSON-RPC over stdin/stdout, with local sockets for streamed bytes.

`process_spec` uses the existing sandbox-driver `SandboxSpec` schema. Source,
resources, kind, volumes, timers, working directory, network, region, environment,
and opaque `provider_config` pass through. Petri supplies the resource name and
`petri.*` labels, and overlays scope environment. Templates must describe an
independent workspace per sandbox; the fixed directory above is suitable only
for a single-workspace example.

Custom providers support ordinary process scopes. Container jobs, services,
and Docker actions need a separate preparation recipe and are refused.
Placement labels must appear in the registration's `placement_labels`.
Restricted networking requires advertised support and confirmation from the
sandbox. A run cannot loosen a template's restriction. Optional `host_address`
declares how the sandbox reaches services on Petri's machine; none is inferred.

Fabro named environments may select `provider = "fixture-provider"`. The name
survives lowering unchanged. Custom provider image, resources, cwd, network,
lifecycle, and label overrides belong in the registration, and are rejected in
workflow environment settings. Workflow environment variables still apply.
`local` remains an alias for `host`; `simulated` is reserved for dry runs.

## Embedded runtime

Build `PluginConfig` with a validated `ProviderKind`, absolute executable path, arguments,
explicit environment, and either a checksum or development opt-in. Pass it to
`PluginSettings::try_new(config)`, then construct
`ProviderRegistration::plugin(settings).process_spec(spec)`.

```rust,ignore
let runtime = Runtime::standard()
    .in_process_providers(linked_defaults)
    .sandbox_provider(registration)?;
```

`sandbox_provider` adds or replaces one binding.
`sandbox_providers(ProviderRegistrations)` replaces the whole set.
`ProviderRegistrations::register` rejects duplicates; `replace` permits a
deliberate override. Missing kinds fail without implicit plugin discovery.
Unused bindings and dry runs launch no plugin. Built-in kinds retain their
existing preparation recipes and fingerprints.

## Recovery

The durable lease's provider kind selects its registration during resume and
prune, even when the current default changes. A custom provider must report a
stable, non-secret health identity. If it cannot, configure a stable, non-secret
`namespace`. When both exist, both contribute to the fingerprint. The library
also offers `NamespacePolicy::Both` to require both. Credentials and executable
location do not contribute, permitting rotation within the same namespace.

Petri verifies each recorded provider group before recovery or pruning mutates
resources. A mismatch refuses attach, create, delete, and orphan sweeping for
that group. A crash invalidates old handles; the next generation attaches and
fences recorded work. Uncertain mutations are reconciled from the ledger and
provider labels, never blindly replayed.

Supply registrations again after restarting Petri. Launch configuration and
templates are not copied to durable run records. Petri masks launch values in
its diagnostics and output; plugin-authored stderr is the plugin's responsibility.

The reference plugin is test scaffolding, not a production isolation boundary.
Tests cover real commands and files, retained restart, pruning, namespace
refusal, and a crash after create but before its reply. CLI tests also kill and
resume a complete Petri process.
