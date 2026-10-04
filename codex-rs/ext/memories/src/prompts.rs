use crate::MEMORY_TOOL_DEVELOPER_INSTRUCTIONS_SUMMARY_TOKEN_LIMIT;
use codex_protocol::MemoryVersion;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_output_truncation::TruncationPolicy;
use codex_utils_output_truncation::truncate_text;
use codex_utils_template::Template;
use codex_utils_template::TemplateParseError;
use codex_utils_template::TemplateRenderError;
use std::sync::Arc;
use std::sync::LazyLock;
use tokio::fs;

static MEMORY_TOOL_DEVELOPER_INSTRUCTIONS_TEMPLATE: LazyLock<
    Result<Template, Arc<TemplateParseError>>,
> = LazyLock::new(|| {
    parse_embedded_template(
        include_str!("../templates/memories/read_path.md"),
        "memories/read_path.md",
    )
});

static MEMORY_V2_TEMPLATE: LazyLock<Result<Template, Arc<TemplateParseError>>> =
    LazyLock::new(|| {
        parse_embedded_template(
            include_str!("../templates/memories/read_path_v2.md"),
            "memories/read_path_v2.md",
        )
    });

fn parse_embedded_template(
    source: &'static str,
    _template_name: &str,
) -> Result<Template, Arc<TemplateParseError>> {
    Template::parse(source).map_err(Arc::new)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SummaryError {
    #[error("selected {version:?} memory summary read failed")]
    Read {
        version: MemoryVersion,
        #[source]
        cause: std::io::Error,
    },
    #[error("selected {version:?} memory prompt template failed")]
    Parse {
        version: MemoryVersion,
        #[source]
        cause: Arc<TemplateParseError>,
    },
    #[error("selected {version:?} memory prompt rendering failed")]
    Render {
        version: MemoryVersion,
        #[source]
        cause: TemplateRenderError,
    },
}

/// Explicit compatibility projection for permitted LOCAL callers only.
/// RemoteRequired must use the fallible boundary and never call this wrapper.
/// Build the memory read-path prompt that is added to developer instructions.
///
/// Large `memory_summary.md` files are truncated at
/// [MEMORY_TOOL_DEVELOPER_INSTRUCTIONS_SUMMARY_TOKEN_LIMIT].
pub(crate) async fn build_memory_tool_developer_instructions(
    codex_home: &AbsolutePathBuf,
    version: MemoryVersion,
) -> Option<String> {
    build_memory_tool_developer_instructions_checked(codex_home, version)
        .await
        .ok()
        .flatten()
}

pub(crate) async fn build_memory_tool_developer_instructions_checked(
    codex_home: &AbsolutePathBuf,
    version: MemoryVersion,
) -> Result<Option<String>, SummaryError> {
    let base_path = codex_home.join(version.directory_name());
    let memory_summary_path = base_path.join("memory_summary.md");
    let summary = match fs::read_to_string(&memory_summary_path).await {
        Ok(summary) => summary,
        Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(cause) => return Err(SummaryError::Read { version, cause }),
    };
    render_memory_summary(version, &base_path.display().to_string(), &summary)
}

fn render_memory_summary(
    version: MemoryVersion,
    base_path: &str,
    summary: &str,
) -> Result<Option<String>, SummaryError> {
    let memory_summary = truncate_text(
        summary.trim(),
        TruncationPolicy::Tokens(MEMORY_TOOL_DEVELOPER_INSTRUCTIONS_SUMMARY_TOKEN_LIMIT),
    );
    if memory_summary.is_empty() {
        return Ok(None);
    }
    let template = match version {
        MemoryVersion::V1 => &MEMORY_TOOL_DEVELOPER_INSTRUCTIONS_TEMPLATE,
        MemoryVersion::V2 => &MEMORY_V2_TEMPLATE,
    };
    let template = template.as_ref().map_err(|cause| SummaryError::Parse {
        version,
        cause: Arc::clone(cause),
    })?;
    render_summary_template(template, version, base_path, &memory_summary).map(Some)
}

fn render_summary_template(
    template: &Template,
    version: MemoryVersion,
    base_path: &str,
    summary: &str,
) -> Result<String, SummaryError> {
    template
        .render([("base_path", base_path), ("memory_summary", summary)])
        .map_err(|cause| SummaryError::Render { version, cause })
}

#[cfg(test)]
#[path = "prompts_tests.rs"]
mod tests;
