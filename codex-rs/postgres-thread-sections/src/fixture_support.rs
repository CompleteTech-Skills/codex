use codex_postgres_runtime::{ConnectionSettings, PoolLimits};
use serde_json::Value;
use std::path::Path;
use std::time::Duration;

pub(crate) fn settings(state: &Path, role: &str) -> ConnectionSettings {
    let receipt: Value = serde_json::from_slice(
        &std::fs::read(state.join("receipt.json")).expect("fixture receipt"),
    )
    .expect("receipt JSON");
    let instance = receipt["instance"]
        .as_str()
        .expect("typed fixture instance");
    assert!(
        instance.len() == 32
            && instance
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "invalid fixture instance"
    );
    let exclusive: Value = serde_json::from_slice(
        &std::fs::read(state.join("section-crud-exclusive.json"))
            .expect("exclusive disposable fixture receipt"),
    )
    .expect("exclusive receipt JSON");
    assert_eq!(
        exclusive["instance"].as_str(),
        Some(instance),
        "exclusive instance mismatch"
    );
    assert_eq!(
        exclusive["purpose"].as_str(),
        Some("section-crud-exclusive")
    );
    assert_eq!(exclusive["disposable"].as_bool(), Some(true));
    let container = exclusive["container_id"]
        .as_str()
        .expect("owned fixture container ID");
    assert!(
        container.len() == 64
            && container
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    let project = receipt["project"].as_str().expect("typed fixture project");
    assert!(
        project.starts_with("codex-pg-")
            && project.len() <= 49
            && project.len() > 9
            && project
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    );
    let engine_id = receipt["engine_id"].as_str().expect("pinned engine");
    assert!(!engine_id.is_empty());
    let engine = std::process::Command::new("docker")
        .args(["info", "--format", "{{.ID}}"])
        .output()
        .expect("inspect Docker engine");
    assert!(engine.status.success());
    assert_eq!(
        std::str::from_utf8(&engine.stdout)
            .expect("engine identity")
            .trim(),
        engine_id
    );
    let inspect = std::process::Command::new("docker")
        .args(["inspect", container])
        .output()
        .expect("inspect owned fixture");
    assert!(inspect.status.success() && inspect.stdout.len() < 1_048_576);
    let containers: Vec<Value> =
        serde_json::from_slice(&inspect.stdout).expect("Docker inspect JSON");
    assert_eq!(containers.len(), 1);
    let actual = &containers[0];
    assert_eq!(actual["Id"].as_str(), Some(container));
    assert_eq!(actual["State"]["Running"].as_bool(), Some(true));
    assert_eq!(
        actual["Config"]["Labels"]["com.completedottech.codex.pg.instance"].as_str(),
        Some(instance)
    );
    assert_eq!(
        actual["Config"]["Labels"]["com.docker.compose.project"].as_str(),
        Some(project)
    );
    let bindings = actual["HostConfig"]["PortBindings"]["5432/tcp"]
        .as_array()
        .expect("fixture port binding");
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0]["HostIp"].as_str(), Some("127.0.0.1"));
    let port = receipt["port"].as_u64().expect("typed fixture port");
    assert!((1024..=65535).contains(&port));
    assert_eq!(
        bindings[0]["HostPort"].as_str(),
        Some(port.to_string().as_str())
    );
    ConnectionSettings {
        host: "localhost".to_string(),
        port: receipt["port"].as_u64().expect("port") as u16,
        database: "codex".to_string(),
        username: format!("codex_{role}"),
        password: std::fs::read_to_string(state.join(format!("secrets/{role}.password")))
            .expect("private credential")
            .trim()
            .to_string()
            .into(),
        ca_certificate: state.join("secrets/ca.crt"),
        limits: PoolLimits {
            connect_timeout: Duration::from_secs(5),
            acquire_timeout: Duration::from_secs(5),
            max_connections: 3,
        },
    }
}

#[cfg(test)]
#[path = "fixture_support_tests.rs"]
mod tests;
