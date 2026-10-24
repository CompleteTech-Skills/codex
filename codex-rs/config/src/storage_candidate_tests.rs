use super::*;
use crate::ConfigRequirements;
use crate::ConfigRequirementsToml;
use pretty_assertions::assert_eq;
use toml::Value as TomlValue;

fn layer(name: ConfigLayerSource, candidate: &str) -> ConfigLayerEntry {
    ConfigLayerEntry::new(name, toml::from_str(candidate).unwrap())
}

fn stack(layers: Vec<ConfigLayerEntry>) -> io::Result<ConfigLayerStack> {
    ConfigLayerStack::new(
        layers,
        ConfigRequirements::default(),
        ConfigRequirementsToml::default(),
    )
}

const LOCAL: &str = "[storage_candidate]\nbackend = 'local_sqlite'\n";
const REMOTE: &str = "[storage_candidate]\nbackend = 'remote_postgres'\nendpoint = 'db.example.test'\nport = 5432\ndatabase = 'codex'\nnamespace = 'history'\nconnect_timeout_seconds = 5\npool_acquire_timeout_seconds = 5\nmax_connections = 8\n[storage_candidate.credential]\nsource = 'environment'\nvariable = 'CODEX_PG_PASSWORD'\n";

#[test]
fn trusted_precedence_selects_complete_candidate_without_activating_it() {
    let user_stack = stack(vec![
        layer(
            ConfigLayerSource::System {
                file: std::env::current_exe().unwrap().try_into().unwrap(),
            },
            REMOTE,
        ),
        layer(
            ConfigLayerSource::User {
                file: std::env::current_exe().unwrap().try_into().unwrap(),
                profile: None,
            },
            LOCAL,
        ),
    ])
    .unwrap();
    assert_eq!(
        user_stack.storage_candidate().unwrap(),
        Some(StorageCandidateProfile::LocalSqlite)
    );
    let effective: crate::config_toml::ConfigToml =
        user_stack.effective_config().try_into().unwrap();
    assert_eq!(
        effective.storage_candidate,
        user_stack.storage_candidate().unwrap()
    );
    assert_eq!(effective.sqlite_home, None);

    let managed = stack(vec![
        layer(
            ConfigLayerSource::User {
                file: std::env::current_exe().unwrap().try_into().unwrap(),
                profile: None,
            },
            LOCAL,
        ),
        layer(ConfigLayerSource::LegacyManagedConfigTomlFromMdm, REMOTE),
    ])
    .unwrap();
    assert!(matches!(
        managed.storage_candidate().unwrap(),
        Some(StorageCandidateProfile::RemotePostgres(_))
    ));
}

#[test]
fn project_and_session_cannot_supply_candidate() {
    let project = layer(
        ConfigLayerSource::Project {
            dot_codex_folder: std::env::current_exe().unwrap().try_into().unwrap(),
        },
        REMOTE,
    );
    for untrusted in [project, layer(ConfigLayerSource::SessionFlags, REMOTE)] {
        let error = stack(vec![untrusted]).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(!error.to_string().contains("db.example.test"));
    }
}

#[test]
fn invalid_trusted_profile_error_is_redacted() {
    let error = stack(vec![layer(
        ConfigLayerSource::LegacyManagedConfigTomlFromMdm,
        "[storage_candidate]\nbackend = 'remote_postgres'\nendpoint = 'secret.example.test'\n",
    )])
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(!error.to_string().contains("secret.example.test"));
}

#[test]
fn managed_destination_locks_whole_selected_candidate() {
    let required = REMOTE.replace("[storage_candidate]", "[storage_destination]")
        .lines()
        .filter(|line| !line.starts_with("connect_timeout_seconds")
            && !line.starts_with("pool_acquire_timeout_seconds")
            && !line.starts_with("max_connections")
            && !line.starts_with("[storage_candidate.credential]")
            && !line.starts_with("source =")
            && !line.starts_with("variable ="))
        .collect::<Vec<_>>()
        .join("\n");
    let requirements: ConfigRequirementsToml = toml::from_str(&required).unwrap();
    let system = ConfigLayerSource::LegacyManagedConfigTomlFromMdm;
    let make_stack = |candidate: &str| ConfigLayerStack::new(
        vec![layer(system.clone(), candidate)],
        ConfigRequirements::default(),
        requirements.clone(),
    );
    let valid = make_stack(REMOTE).unwrap();
    assert_eq!(valid.storage_candidate().unwrap().unwrap().destination(),
        requirements.storage_destination.clone().unwrap());
    let effective: crate::config_toml::ConfigToml = valid.effective_config().try_into().unwrap();
    assert_eq!(effective.sqlite_home, None);
    make_stack(&REMOTE.replace("CODEX_PG_PASSWORD", "HOST_OWNED_SECRET")).unwrap();
    ConfigLayerStack::new(
        vec![
            layer(ConfigLayerSource::User {
                file: std::env::current_exe().unwrap().try_into().unwrap(),
                profile: None,
            }, LOCAL),
            layer(system.clone(), REMOTE),
        ],
        ConfigRequirements::default(),
        requirements.clone(),
    ).unwrap();
    for invalid in [
        LOCAL.to_string(),
        REMOTE.replace("db.example.test", "other.example.test"),
        REMOTE.replace("port = 5432", "port = 5433"),
        REMOTE.replace("database = 'codex'", "database = 'other'"),
        REMOTE.replace("namespace = 'history'", "namespace = 'other'"),
    ]
    {
        let error = make_stack(&invalid).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(!error.to_string().contains("db.example.test"));
    }
    let mut with_ca: TomlValue = toml::from_str(REMOTE).unwrap();
    let mut tls = toml::map::Map::new();
    tls.insert("ca_certificate".to_string(), TomlValue::String(
        std::env::current_exe().unwrap().display().to_string()));
    with_ca["storage_candidate"].as_table_mut().unwrap()
        .insert("tls".to_string(), TomlValue::Table(tls));
    let error = ConfigLayerStack::new(
        vec![ConfigLayerEntry::new(system.clone(), with_ca)],
        ConfigRequirements::default(),
        requirements.clone(),
    ).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    let weak_tls = format!("{REMOTE}[storage_candidate.tls]\nverification = 'disable'\n");
    assert_eq!(make_stack(&weak_tls).unwrap_err().kind(), io::ErrorKind::InvalidData);
}

#[test]
fn managed_local_destination_accepts_absent_candidate() {
    let requirements: ConfigRequirementsToml =
        toml::from_str("[storage_destination]\nbackend = 'local_sqlite'").unwrap();
    ConfigLayerStack::new(
        vec![],
        ConfigRequirements::default(),
        requirements,
    ).unwrap();
}

#[test]
fn managed_destination_rejects_user_edits_and_hot_layer_refresh() {
    let requirements: ConfigRequirementsToml =
        toml::from_str("[storage_destination]\nbackend = 'local_sqlite'").unwrap();
    let file = std::env::current_exe().unwrap().try_into().unwrap();
    let user = ConfigLayerSource::User { file, profile: None };
    let locked = ConfigLayerStack::new(
        vec![layer(user.clone(), LOCAL)],
        ConfigRequirements::default(),
        requirements,
    ).unwrap();
    let remote: TomlValue = toml::from_str(REMOTE).unwrap();
    let file = locked.get_user_config_file().unwrap();
    assert_eq!(locked.with_user_config(file, remote.clone()).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied);
    let incoming = stack(vec![ConfigLayerEntry::new(user, remote)]).unwrap();
    assert_eq!(locked.with_user_layer_from(&incoming).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied);
}
