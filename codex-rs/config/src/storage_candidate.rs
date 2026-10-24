//! Candidate storage settings are parsed only from host-owned config layers.
//! They never select the authoritative backend.

use crate::ConfigLayerEntry;
use crate::ConfigLayerSource;
use crate::ConfigLayerStack;
use codex_storage_authority::StorageCandidateProfile;
use codex_storage_authority::StorageDestination;
use std::io;
use toml::Value as TomlValue;

pub const STORAGE_CANDIDATE_KEY: &str = "storage_candidate";

/// TOML parser diagnostics can echo source lines; suppress them for candidate files.
pub(crate) fn redacted_parse_error(contents: &str) -> Option<io::Error> {
    contents.contains(STORAGE_CANDIDATE_KEY).then(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid configuration with storage candidate",
        )
    })
}

pub(crate) fn validate_storage_candidate_layer(layer: &ConfigLayerEntry) -> io::Result<()> {
    if layer.config.get(STORAGE_CANDIDATE_KEY).is_none() {
        return Ok(());
    }
    if matches!(
        layer.name,
        ConfigLayerSource::Project { .. } | ConfigLayerSource::SessionFlags
    ) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "storage candidate is forbidden in project and session configuration",
        ));
    }
    validate_storage_candidate_value(&layer.config)
}

pub(crate) fn validate_storage_candidate_value(value: &TomlValue) -> io::Result<()> {
    let Some(candidate) = value.get(STORAGE_CANDIDATE_KEY) else {
        return Ok(());
    };
    let _: StorageCandidateProfile = candidate
        .clone()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid storage candidate"))?;
    Ok(())
}

impl ConfigLayerStack {
    pub(crate) fn validate_storage_destination(&self) -> io::Result<()> {
        let Some(required) = &self.requirements_toml().storage_destination else {
            return Ok(());
        };
        required.validate().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid managed storage destination")
        })?;
        let selected = self
            .storage_candidate()?
            .map(|candidate| candidate.destination())
            .unwrap_or(StorageDestination::LocalSqlite);
        if &selected != required {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "storage candidate does not match managed storage destination",
            ));
        }
        Ok(())
    }

    /// Returns the highest-precedence validated candidate from a trusted layer.
    /// This proposal has no effect on the active SQLite backend.
    pub fn storage_candidate(&self) -> io::Result<Option<StorageCandidateProfile>> {
        self.layers_high_to_low()
            .find_map(|layer| layer.config.get(STORAGE_CANDIDATE_KEY))
            .map(|candidate| {
                candidate.clone().try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid storage candidate")
                })
            })
            .transpose()
    }
}

#[cfg(test)]
#[path = "storage_candidate_tests.rs"]
mod tests;
