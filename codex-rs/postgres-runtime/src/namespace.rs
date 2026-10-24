//! Validated identifiers for a preprovisioned, separate PostgreSQL namespace.

/// A schema and its distinct role names. The host must supply the matching
/// migrator credentials; this type grants no access by itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedNamespace {
    pub(crate) schema: String,
    pub(crate) owner: String,
    pub(crate) migrator: String,
    pub(crate) runtime: String,
}

/// An invalid or unsupported namespace identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidNamespace;

impl NamedNamespace {
    /// Login required for read-only namespace compatibility preflight.
    pub fn migrator_login(&self) -> &str {
        &self.migrator
    }

    /// Accept `codex_storage_<stem>` with a short, lowercase ASCII stem.
    /// Roles are derived as `codex_<stem>_{owner,migrator,runtime}`.
    pub fn new(schema: &str) -> Result<Self, InvalidNamespace> {
        let stem = schema
            .strip_prefix("codex_storage_")
            .ok_or(InvalidNamespace)?;
        let bytes = stem.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 48
            || !bytes.first().is_some_and(u8::is_ascii_alphanumeric)
            || !bytes.last().is_some_and(u8::is_ascii_alphanumeric)
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
        {
            return Err(InvalidNamespace);
        }
        Ok(Self {
            schema: schema.to_owned(),
            owner: format!("codex_{stem}_owner"),
            migrator: format!("codex_{stem}_migrator"),
            runtime: format!("codex_{stem}_runtime"),
        })
    }

    /// Validated schema identifier for parameterized domain adapters.
    pub fn schema_name(&self) -> &str {
        &self.schema
    }

    pub(crate) fn quoted_schema(&self) -> String {
        format!("\"{}\"", self.schema)
    }

    pub(crate) fn quoted_owner(&self) -> String {
        format!("\"{}\"", self.owner)
    }

    pub(crate) fn quoted_runtime(&self) -> String {
        format!("\"{}\"", self.runtime)
    }
}
