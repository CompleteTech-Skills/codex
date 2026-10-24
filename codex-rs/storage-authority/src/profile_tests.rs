use super::*;
use pretty_assertions::assert_eq;
use serde_json::json;

fn remote_profile() -> serde_json::Value {
    json!({
        "backend": "remote_postgres",
        "endpoint": "db.example.test",
        "port": 5432,
        "database": "codex",
        "namespace": "codex_storage",
        "credential": {"source": "environment", "variable": "CODEX_PG_PASSWORD"},
        "connect_timeout_seconds": 5,
        "pool_acquire_timeout_seconds": 10,
        "max_connections": 8
    })
}

#[test]
fn candidate_round_trip_keeps_local_distinct_and_defaults_to_verified_tls() {
    let local: StorageCandidateProfile =
        serde_json::from_value(json!({"backend": "local_sqlite"})).unwrap();
    assert_eq!(local, StorageCandidateProfile::LocalSqlite);
    assert_eq!(
        serde_json::to_value(local).unwrap(),
        json!({"backend": "local_sqlite"})
    );

    let remote: StorageCandidateProfile = serde_json::from_value(remote_profile()).unwrap();
    let StorageCandidateProfile::RemotePostgres(profile) = &remote else {
        panic!("remote candidate must retain its backend type")
    };
    assert_eq!(profile.tls, TlsSettings::default());
    assert_eq!(profile.connect_timeout_seconds, 5);
    assert_eq!(
        serde_json::from_value::<StorageCandidateProfile>(serde_json::to_value(&remote).unwrap())
            .unwrap(),
        remote
    );
}

#[test]
fn rejects_endpoint_identifier_and_pool_redirection() {
    let mut value = remote_profile();
    for endpoint in [
        "user:secret@db.example.test",
        "db.example.test:5432",
        "bad..host",
        "127.0.0.999",
        "-bad.example",
        "db.example/other",
        "",
    ] {
        value["endpoint"] = json!(endpoint);
        assert!(serde_json::from_value::<StorageCandidateProfile>(value.clone()).is_err());
    }
    value = remote_profile();
    value["namespace"] = json!("public; drop schema public");
    assert!(serde_json::from_value::<StorageCandidateProfile>(value.clone()).is_err());
    value = remote_profile();
    value["port"] = json!(0);
    assert!(serde_json::from_value::<StorageCandidateProfile>(value.clone()).is_err());
    value = remote_profile();
    value["connect_timeout_seconds"] = json!(31);
    assert!(serde_json::from_value::<StorageCandidateProfile>(value.clone()).is_err());
    value = remote_profile();
    value["max_connections"] = json!(0);
    assert!(serde_json::from_value::<StorageCandidateProfile>(value).is_err());
}

#[test]
fn credential_references_are_typed_and_never_debugged() {
    let mut value = remote_profile();
    value["credential"] = json!({"source": "keyring", "id": "host_credential_1"});
    let profile: StorageCandidateProfile = serde_json::from_value(value.clone()).unwrap();
    let debug = format!("{profile:?}");
    assert!(!debug.contains("host_credential_1"));
    assert!(!debug.contains("db.example.test"));
    assert!(
        !format!(
            "{:?}",
            CredentialRef::parse("host_credential_1".to_string()).unwrap()
        )
        .contains("host_credential_1")
    );

    value["credential"]["id"] = json!("password=secret");
    let error = serde_json::from_value::<StorageCandidateProfile>(value.clone()).unwrap_err();
    assert!(!error.to_string().contains("secret"));
    value = remote_profile();
    value["credential"]["variable"] = json!("$(curl attacker)");
    let error = serde_json::from_value::<StorageCandidateProfile>(value).unwrap_err();
    assert!(!error.to_string().contains("attacker"));
}

#[test]
fn unknown_secrets_and_weak_tls_are_rejected() {
    let mut value = remote_profile();
    value["password"] = json!("synthetic-secret");
    assert!(serde_json::from_value::<StorageCandidateProfile>(value).is_err());
    let mut value = remote_profile();
    value["tls"] = json!({"verification": "disable"});
    assert!(serde_json::from_value::<StorageCandidateProfile>(value).is_err());
    let mut value = remote_profile();
    value["tls"] = json!({"ca_certificate": "relative-ca.crt"});
    assert!(serde_json::from_value::<StorageCandidateProfile>(value).is_err());
    let mut value = remote_profile();
    value["credential"]["password"] = json!("synthetic-secret");
    assert!(serde_json::from_value::<StorageCandidateProfile>(value).is_err());
}

#[test]
fn full_verification_accepts_an_ip_and_absolute_ca_path() {
    let mut value = remote_profile();
    value["endpoint"] = json!("2001:db8::1");
    value["tls"] = json!({
        "verification": "verify_full",
        "ca_certificate": std::env::current_dir().unwrap().join("ca.crt")
    });
    let parsed: StorageCandidateProfile = serde_json::from_value(value).unwrap();
    let StorageCandidateProfile::RemotePostgres(profile) = parsed else {
        panic!("expected remote candidate")
    };
    assert_eq!(profile.endpoint, "2001:db8::1");
    assert_eq!(profile.tls.verification, TlsVerification::VerifyFull);
}

#[test]
fn destination_projection_excludes_credential_and_connection_limits() {
    let profile: StorageCandidateProfile = serde_json::from_value(remote_profile()).unwrap();
    let destination = profile.destination();
    destination.validate().unwrap();
    let expected: StorageDestination = serde_json::from_value(json!({
        "backend": "remote_postgres",
        "endpoint": "db.example.test",
        "port": 5432,
        "database": "codex",
        "namespace": "codex_storage"
    })).unwrap();
    assert_eq!(destination, expected);
    let debug = format!("{destination:?}");
    assert!(!debug.contains("db.example.test"));
    assert!(!debug.contains("CODEX_PG_PASSWORD"));
}
