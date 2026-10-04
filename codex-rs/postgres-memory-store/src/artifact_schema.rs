//! Unregistered portable artifact definitions, AFTER authority20 and V2 database21.
//! No migration number, grants, declaration, execution or readiness permission.
//! The caller must associate the selected native namespace/origin and validate
//! complete manifests, digests, leases and CAS publication in an original task.

/// Fixed relation names for future protected catalogue registration.
pub(crate) const ARTIFACT_TABLES: [&str; 4] = [
    "memory_artifact_heads",
    "memory_artifact_revisions",
    "memory_artifact_entries",
    "memory_artifact_chunks",
];

/// Quote one identifier only. This is syntax, not namespace/origin authority.
pub(crate) fn render_artifact_schema(namespace: &str) -> String {
    let schema = format!("\"{}\"", namespace.replace('"', "\"\""));
    ARTIFACT_SCHEMA.replace("@SCHEMA@", &schema)
}

const ARTIFACT_SCHEMA: &str = r#"
CREATE TABLE @SCHEMA@.memory_artifact_revisions (
    dataset_id text COLLATE "C" NOT NULL,
    memory_version integer NOT NULL CHECK (memory_version IN (1,2)),
    revision_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    base_revision_id uuid,
    storage_generation bigint NOT NULL CHECK (storage_generation >= 0),
    root_present boolean NOT NULL,
    publication_kind text NOT NULL CHECK (publication_kind IN ('initial','consolidation','ad_hoc','migration')),
    manifest_sha256 bytea NOT NULL CHECK (octet_length(manifest_sha256) = 32),
    file_count bigint NOT NULL CHECK (file_count >= 0),
    total_bytes bigint NOT NULL CHECK (total_bytes >= 0),
    lease_token uuid,
    selected_input_sha256 bytea CHECK (selected_input_sha256 IS NULL OR octet_length(selected_input_sha256) = 32),
    published boolean NOT NULL DEFAULT false,
    PRIMARY KEY (dataset_id,memory_version,revision_id),
    UNIQUE (dataset_id,memory_version,operation_id),
    UNIQUE (dataset_id,memory_version,revision_id,published),
    FOREIGN KEY (dataset_id,memory_version,base_revision_id)
        REFERENCES @SCHEMA@.memory_artifact_revisions(dataset_id,memory_version,revision_id),
    CHECK (root_present OR (file_count = 0 AND total_bytes = 0)),
    CHECK (publication_kind <> 'consolidation' OR (lease_token IS NOT NULL AND selected_input_sha256 IS NOT NULL))
);
CREATE TABLE @SCHEMA@.memory_artifact_entries (
    dataset_id text COLLATE "C" NOT NULL,
    memory_version integer NOT NULL,
    revision_id uuid NOT NULL,
    path text COLLATE "C" NOT NULL,
    byte_length bigint NOT NULL CHECK (byte_length >= 0),
    content_sha256 bytea NOT NULL CHECK (octet_length(content_sha256) = 32),
    PRIMARY KEY (dataset_id,memory_version,revision_id,path),
    FOREIGN KEY (dataset_id,memory_version,revision_id)
        REFERENCES @SCHEMA@.memory_artifact_revisions(dataset_id,memory_version,revision_id),
    CHECK (path <> '' AND path !~ '(^/|/$|//|(^|/)[.][.]?(/|$)|:)' AND strpos(path, chr(92)) = 0)
);
CREATE TABLE @SCHEMA@.memory_artifact_chunks (
    dataset_id text COLLATE "C" NOT NULL,
    memory_version integer NOT NULL,
    revision_id uuid NOT NULL,
    path text COLLATE "C" NOT NULL,
    chunk_ordinal integer NOT NULL CHECK (chunk_ordinal >= 0),
    byte_offset bigint NOT NULL CHECK (byte_offset >= 0),
    payload bytea NOT NULL CHECK (octet_length(payload) BETWEEN 1 AND 65536),
    PRIMARY KEY (dataset_id,memory_version,revision_id,path,chunk_ordinal),
    UNIQUE (dataset_id,memory_version,revision_id,path,byte_offset),
    FOREIGN KEY (dataset_id,memory_version,revision_id,path)
        REFERENCES @SCHEMA@.memory_artifact_entries(dataset_id,memory_version,revision_id,path)
);
CREATE TABLE @SCHEMA@.memory_artifact_heads (
    dataset_id text COLLATE "C" NOT NULL,
    memory_version integer NOT NULL CHECK (memory_version IN (1,2)),
    revision_id uuid NOT NULL,
    published boolean NOT NULL DEFAULT true CHECK (published),
    PRIMARY KEY (dataset_id,memory_version),
    FOREIGN KEY (dataset_id,memory_version,revision_id,published)
        REFERENCES @SCHEMA@.memory_artifact_revisions(dataset_id,memory_version,revision_id,published)
);
"#;
