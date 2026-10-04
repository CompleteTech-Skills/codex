use std::sync::Arc;

use codex_core::config::Config;
use codex_core::context::ContextualUserFragment;
use codex_core::context::MemoryContextFragment;
use codex_extension_api::ConfigContributor;
use codex_extension_api::ContentItemKind;
use codex_extension_api::ContextContributor;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_extension_api::PromptFragment;
use codex_extension_api::ThreadLifecycleContributor;
use codex_extension_api::ThreadStartInput;
use codex_extension_api::ToolContributor;
use codex_features::Feature;
use codex_otel::MetricsClient;
use codex_protocol::MemoryVersion;
use codex_utils_absolute_path::AbsolutePathBuf;

use crate::local::LocalMemoriesBackend;
use crate::prompts::build_memory_tool_developer_instructions;
use crate::tools;

/// Contributes Codex memory read-path prompt context and memory read tools.
#[derive(Clone, Default)]
pub(crate) struct MemoriesExtension {
    metrics_client: Option<MetricsClient>,
}

impl MemoriesExtension {
    fn new(metrics_client: Option<MetricsClient>) -> Self {
        Self { metrics_client }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MemoriesExtensionConfig {
    pub(crate) enabled: bool,
    pub(crate) dedicated_tools: bool,
    pub(crate) codex_home: AbsolutePathBuf,
    pub(crate) version: MemoryVersion,
}

impl MemoriesExtensionConfig {
    fn from_config(config: &Config) -> Self {
        Self {
            enabled: config.features.enabled(Feature::MemoryTool) && config.memories.use_memories,
            dedicated_tools: config.memories.dedicated_tools,
            codex_home: config.codex_home.clone(),
            version: config.memories.version,
        }
    }
}

/// Immutable backend association captured before context/tool lookup. A
/// RemoteRequired value cannot fall through to a local filesystem backend.
enum SelectedSummaryBackend {
    Local,
    RemoteRequired(Option<Arc<codex_core::RemoteBackend>>),
}
impl SelectedSummaryBackend {
    fn capture() -> Self {
        match codex_core::remote_backend() {
            Some(original) => Self::RemoteRequired(Some(Arc::new(original.clone()))),
            None => Self::Local,
        }
    }
}
struct MissingRemoteSummaryProvider {
    version: MemoryVersion,
    original: Option<Arc<codex_core::RemoteBackend>>,
}
impl std::fmt::Debug for MissingRemoteSummaryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.original;
        f.debug_struct("MissingRemoteSummaryProvider")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for MissingRemoteSummaryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "selected {:?} remote memory artifact provider is unavailable",
            self.version
        )
    }
}
impl std::error::Error for MissingRemoteSummaryProvider {}

impl ContextContributor for MemoriesExtension {
    fn contribute_thread_context_checked<'a>(
        &'a self,
        _session_store: &'a ExtensionData,
        thread_store: &'a ExtensionData,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::io::Result<Vec<PromptFragment>>> + Send + 'a>,
    > {
        Box::pin(async move {
            let Some(config) = thread_store.get::<MemoriesExtensionConfig>() else {
                return Ok(Vec::new());
            };
            if !config.enabled {
                return Ok(Vec::new());
            }

            let selected = thread_store.get_or_init(SelectedSummaryBackend::capture);
            if let SelectedSummaryBackend::RemoteRequired(original) = selected.as_ref() {
                // No native artifact implementation exists yet. Refuse before
                // ANY local summary read; retain the same selected association.
                return Err(std::io::Error::other(MissingRemoteSummaryProvider {
                    version: config.version,
                    original: original.clone(),
                }));
            }
            let Some(instructions) =
                build_memory_tool_developer_instructions(&config.codex_home, config.version).await
            else {
                return Ok(Vec::new());
            };
            let instructions = match config.version {
                MemoryVersion::V1 => vec![instructions],
                MemoryVersion::V2 => {
                    // Keep the complete summary while respecting each fragment's byte cap.
                    let mut remaining = instructions.as_str();
                    let mut fragments = Vec::new();
                    while !remaining.is_empty() {
                        let end = remaining.floor_char_boundary(remaining.len().min(8_900));
                        fragments.push(
                            MemoryContextFragment::ReadInstructions(remaining[..end].to_string())
                                .render(),
                        );
                        remaining = &remaining[end..];
                    }
                    fragments
                }
            };
            Ok(instructions
                .into_iter()
                .map(|instructions| {
                    PromptFragment::developer_policy(
                        instructions,
                        ContentItemKind("memories.instructions".to_string()),
                    )
                })
                .collect())
        })
    }
}

impl ThreadLifecycleContributor<Config> for MemoriesExtension {
    fn on_thread_start<'a>(
        &'a self,
        input: ThreadStartInput<'a, Config>,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            input
                .thread_store
                .get_or_init(SelectedSummaryBackend::capture);
            input
                .thread_store
                .insert(MemoriesExtensionConfig::from_config(input.config));
        })
    }
}

impl ConfigContributor<Config> for MemoriesExtension {
    fn on_config_changed(
        &self,
        _session_store: &ExtensionData,
        thread_store: &ExtensionData,
        _previous_config: &Config,
        new_config: &Config,
    ) {
        let mut config = MemoriesExtensionConfig::from_config(new_config);
        if let Some(previous) = thread_store.get::<MemoriesExtensionConfig>() {
            // The initial summary and retrieval tools must use the same namespace.
            config.version = previous.version;
        }
        thread_store.insert(config);
    }
}

impl ToolContributor for MemoriesExtension {
    fn tools(
        &self,
        _session_store: &ExtensionData,
        thread_store: &ExtensionData,
    ) -> Vec<
        Arc<dyn for<'call> codex_extension_api::ToolExecutor<codex_extension_api::ToolCall<'call>>>,
    > {
        let Some(config) = thread_store.get::<MemoriesExtensionConfig>() else {
            return Vec::new();
        };
        if !config.enabled || !config.dedicated_tools {
            return Vec::new();
        }

        let selected = thread_store.get_or_init(SelectedSummaryBackend::capture);
        if matches!(selected.as_ref(), SelectedSummaryBackend::RemoteRequired(_)) {
            // Checked context blocks the actual turn with an owning refusal.
            // Do not construct local tool backends in that remote mode.
            return Vec::new();
        }
        tools::memory_tools(
            LocalMemoriesBackend::from_memory_root(
                config
                    .codex_home
                    .join(config.version.directory_name())
                    .to_path_buf(),
            ),
            self.metrics_client.clone(),
        )
    }
}

/// Installs the memories extension contributors into the extension registry.
pub fn install(
    registry: &mut ExtensionRegistryBuilder<Config>,
    metrics_client: Option<MetricsClient>,
) {
    let extension = Arc::new(MemoriesExtension::new(metrics_client));
    registry.thread_lifecycle_contributor(extension.clone());
    registry.config_contributor(extension.clone());
    registry.prompt_contributor(extension.clone());
    registry.tool_contributor(extension);
}

#[cfg(test)]
mod checked_backend_tests {
    use super::*;
    #[tokio::test]
    async fn remote_required_refuses_before_local_summary_or_tool_lookup() {
        let home = tempfile::tempdir().unwrap();
        let home = AbsolutePathBuf::from_absolute_path(home.path()).unwrap();
        for version in [MemoryVersion::V1, MemoryVersion::V2] {
            let directory = home.join(version.directory_name());
            tokio::fs::create_dir_all(&directory).await.unwrap();
            tokio::fs::write(directory.join("memory_summary.md"), [0xff])
                .await
                .unwrap();
            let thread = ExtensionData::new("thread");
            thread.insert(MemoriesExtensionConfig {
                enabled: true,
                dedicated_tools: true,
                codex_home: home.clone(),
                version,
            });
            thread.insert(SelectedSummaryBackend::RemoteRequired(None));
            let session = ExtensionData::new("session");
            let extension = MemoriesExtension::default();
            let error = extension
                .contribute_thread_context_checked(&session, &thread)
                .await
                .unwrap_err();
            let cause = error
                .get_ref()
                .unwrap()
                .downcast_ref::<MissingRemoteSummaryProvider>()
                .unwrap();
            assert_eq!(cause.version, version);
            assert!(extension.tools(&session, &thread).is_empty());
            // A local read would instead expose InvalidData; the unavailable
            // provider is the actual earlier checked boundary.
            assert_eq!(error.kind(), std::io::ErrorKind::Other);
        }
    }
}
