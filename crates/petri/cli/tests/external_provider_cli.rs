//! Complete Petri processes using an explicitly registered, unfamiliar plugin.

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};
use std::{env, fs, thread};

use serde_json::json;
use testkit::RunDir;

fn petri() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_petri"));
    command.env_clear().env("PATH", "/usr/bin:/bin");
    command
}

fn config(dir: &RunDir, executable: &Path, extra_env: &str) -> PathBuf {
    let path = dir.path().join("providers.toml");
    let quoted = |path: &Path| json!(path.to_string_lossy()).to_string();
    fs::write(
        &path,
        format!(
            r#"
[providers.fixture-provider]
executable = {}
args = ["--fixture"]
dev = true
[providers.fixture-provider.env]
PATH = "/usr/bin:/bin"
FIXTURE_ROOT = {}
FIXTURE_IDENTITY = "account-a"
{extra_env}
[providers.fixture-provider.process_spec]
source = "host_directory"
working_directory = {}
workspace_ownership = "managed"
"#,
            quoted(executable),
            quoted(&dir.path().join("backend")),
            quoted(&dir.path().join("workspace"))
        ),
    )
    .expect("CLI fixture operation");
    path
}

fn fixture() -> PathBuf {
    let path = env::current_exe()
        .expect("CLI fixture operation")
        .parent()
        .expect("CLI fixture operation")
        .parent()
        .expect("CLI fixture operation")
        .join("fixture-provider");
    assert!(
        path.is_file(),
        "build the protocol fixture with cargo build -p petri-testkit --bin fixture-provider (the workspace test gate builds it)"
    );
    path
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn calls(dir: &RunDir) -> String {
    fs::read_to_string(dir.path().join("backend/calls")).unwrap_or_default()
}

#[test]
fn native_run_and_prune_keep_the_recorded_custom_provider() {
    let dir = RunDir::new("external-cli-native");
    let config = config(&dir, &fixture(), "");
    let workflow = dir.path().join("workflow.petri.yaml");
    fs::write(
        &workflow,
        "name: custom\nnodes:\n  write:\n    run: printf plugin-output > result\n",
    )
    .expect("CLI fixture operation");
    let run = dir.path().join("run");
    let output = petri()
        .env(
            "FIXTURE_FAIL_CREATE",
            "ambient-environment-must-not-reach-plugin",
        )
        .args([
            "run",
            "--retain",
            "always",
            "--backend",
            "fixture-provider",
            "--sandbox-providers",
        ])
        .arg(&config)
        .arg("--run-dir")
        .arg(&run)
        .arg(&workflow)
        .output()
        .expect("CLI fixture operation");
    assert_success(&output);
    assert_eq!(
        fs::read(dir.path().join("workspace/result")).expect("CLI fixture operation"),
        b"plugin-output"
    );
    let output = petri()
        .args([
            "sandbox",
            "prune",
            "--backend",
            "host",
            "--sandbox-providers",
        ])
        .arg(&config)
        .arg("--run-dir")
        .arg(&run)
        .output()
        .expect("CLI fixture operation");
    assert_success(&output);
    assert!(
        !dir.path().join("workspace").exists(),
        "prune deletes managed workspace"
    );
    assert_eq!(
        calls(&dir).lines().filter(|line| *line == "create").count(),
        1
    );
    assert!(calls(&dir).lines().any(|line| line == "delete"));
}

fn fabro(dir: &RunDir, script: &str) -> PathBuf {
    let workflow = dir.path().join("workflow.fabro");
    fs::write(
        &workflow,
        format!(
            r"digraph G {{
        start [shape=Mdiamond]
        exit [shape=Msquare]
        work [shape=parallelogram, script={}]
        start -> work -> exit
    }}",
            json!(script)
        ),
    )
    .expect("CLI fixture operation");
    fs::write(dir.path().join("workflow.toml"), "[run.environment]\nid = 'selected'\n[environments.selected]\nprovider = 'fixture-provider'\n").expect("CLI fixture operation");
    workflow
}

#[test]
fn fabro_provider_selection_and_dry_run_preserve_custom_names() {
    let dir = RunDir::new("external-cli-fabro");
    let config = config(&dir, &fixture(), "");
    let workflow = fabro(&dir, "printf fabro-output > result");
    let output = petri()
        .args(["run", "--retain", "always", "--sandbox-providers"])
        .arg(&config)
        .arg("--run-dir")
        .arg(dir.path().join("run"))
        .arg(&workflow)
        .output()
        .expect("CLI fixture operation");
    assert_success(&output);
    assert_eq!(
        fs::read(dir.path().join("workspace/result")).expect("CLI fixture operation"),
        b"fabro-output"
    );
    let before = calls(&dir);
    let missing_config = self::config(&dir, Path::new("/missing/fixture-provider"), "");
    let output = petri()
        .args(["run", "--dry-run", "--sandbox-providers"])
        .arg(&missing_config)
        .arg("--run-dir")
        .arg(dir.path().join("dry-run"))
        .arg(&workflow)
        .output()
        .expect("CLI fixture operation");
    assert_success(&output);
    assert_eq!(
        calls(&dir),
        before,
        "dry run never resolves or launches the plugin"
    );
}

#[test]
fn a_fresh_process_resumes_the_recorded_provider_after_a_crash() {
    let dir = RunDir::new("external-cli-resume");
    let config = config(&dir, &fixture(), "");
    let marker = dir.path().join("continue");
    let ready = dir.path().join("ready");
    let workflow = fabro(
        &dir,
        &format!(
            "touch '{}'; while [ ! -f '{}' ]; do sleep 0.05; done; printf resumed > result",
            ready.display(),
            marker.display()
        ),
    );
    let run = dir.path().join("run");
    let mut child = petri()
        .args(["run", "--retain", "always", "--sandbox-providers"])
        .arg(&config)
        .arg("--run-dir")
        .arg(&run)
        .arg(&workflow)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("CLI fixture operation");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready.exists() {
        if child.try_wait().expect("CLI fixture operation").is_some() || Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the plugin's command never reached its readiness marker");
        }
        thread::sleep(Duration::from_millis(20));
    }
    child.kill().expect("CLI fixture operation");
    child.wait().expect("CLI fixture operation");
    fs::write(&marker, "continue").expect("CLI fixture operation");
    let output = petri()
        .args([
            "resume",
            "--retain",
            "always",
            "--backend",
            "host",
            "--sandbox-providers",
        ])
        .arg(&config)
        .arg("--run-dir")
        .arg(&run)
        .output()
        .expect("CLI fixture operation");
    assert_success(&output);
    assert_eq!(
        fs::read(dir.path().join("workspace/result")).expect("CLI fixture operation"),
        b"resumed"
    );
    assert_eq!(
        calls(&dir).lines().filter(|line| *line == "create").count(),
        1,
        "resume attaches and fences, without creating again"
    );
    let output = petri()
        .args(["sandbox", "prune", "--sandbox-providers"])
        .arg(&config)
        .arg("--run-dir")
        .arg(&run)
        .output()
        .expect("CLI fixture operation");
    assert_success(&output);
}

#[test]
fn launch_values_are_redacted_from_returned_errors_and_durable_files() {
    let dir = RunDir::new("external-cli-redaction");
    let sentinel = "plugin-credential-must-not-leak";
    let config = config(
        &dir,
        &fixture(),
        &format!("FIXTURE_FAIL_CREATE = '{sentinel}'"),
    );
    let workflow = fabro(&dir, "true");
    let run = dir.path().join("run");
    let output = petri()
        .env(
            "PETRI_LOG",
            "sandbox_driver_protocol::client::provider=trace",
        )
        .args(["run", "--sandbox-providers"])
        .arg(&config)
        .arg("--run-dir")
        .arg(&run)
        .arg(&workflow)
        .output()
        .expect("CLI fixture operation");
    assert!(!output.status.success());
    assert!(
        !String::from_utf8_lossy(&output.stderr).contains(sentinel),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains(sentinel));
    assert_clean_tree(&run, sentinel);
}

fn assert_clean_tree(path: &Path, sentinel: &str) {
    for entry in fs::read_dir(path).expect("CLI fixture operation") {
        let path = entry.expect("CLI fixture operation").path();
        if path.is_dir() {
            assert_clean_tree(&path, sentinel);
        } else {
            assert!(
                !String::from_utf8_lossy(&fs::read(&path).expect("CLI fixture operation"))
                    .contains(sentinel),
                "{} leaked registration values",
                path.display()
            );
        }
    }
}
