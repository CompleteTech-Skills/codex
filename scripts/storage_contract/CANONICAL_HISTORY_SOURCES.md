# Canonical history source map — partial issue #2

Pinned fork main: `b9eee2aea67f9171d8e89c836a9e7f84355c9bee`.
This is a read-only code inventory, not an exporter, portable artifact format,
backend interface, or proof that a second host can resume a thread. Existing
SQL/session-index/host fixtures are documented separately.

## Source identity

The following Git blob IDs were read from that exact commit with
`git rev-parse HEAD:<path>`. They identify the inspected implementation, not
the contents of a future user's home. Recheck them after an upstream rebase.

| Source file under `codex-rs/` | Git blob |
|---|---|
| `history/src/lib.rs` | `090ca9178216f64b4f2815eedc716d6cc45da582` |
| `history/src/rollout_payload.rs` | `d740430a190e5a67c7b06333d95d96e89a0bbdb7` |
| `protocol/src/protocol.rs` | `abc4e90847636bfe51e6275ede528a13616c8573` |
| `rollout/src/recorder.rs` | `cd05f6149b3d0ff97addf43ec243b7452a9de343` |
| `rollout/src/lib.rs` | `6bcbf57fde3b524a6e09aef1719d5fd22a617998` |
| `rollout/src/compression.rs` | `ba7eb24c5b91b47683e5516a2fb4a5122e55b0bc` |
| `rollout/src/model_context.rs` | `3a5854165dc361761fe56c8647219dd66791fb62` |
| `rollout/src/rollout_reference_index.rs` | `a55d2344f9243c897c156e593c878dc8658627ae` |
| `rollout/src/list.rs` | `13c94617a16550fd2c7d683939234749e4f1af48` |
| `rollout/src/session_index.rs` | `ecfcf44796bd86b21598fa970f5ff3d59b1b9a8e` |
| `thread-store/src/local/rollout_lineage.rs` | `ae3e1bec62f9fa24b20348afc3b9fc638760c6f6` |
| `thread-store/src/local/model_context.rs` | `7e898add6dffa9ebe17d0150b22769fe4a50cb10` |
| `thread-store/src/local/thread_history_materialization.rs` | `34eeede51b4169b0fc4978b2501a8216bcea9983` |
| `thread-store/src/local/thread_history.rs` | `81364f514203347e10987016f264f2d2fe211fe2` |
| `thread-store/src/local/thread_history/read.rs` | `dad2a26467783fba2b5f94ce8b121b96c5a19424` |
| `thread-store/src/local/thread_history/search.rs` | `f93fe61eb30d2675a768e82abccfd476d40dd56c` |
| `thread-store/src/local/thread_history/realtime.rs` | `0dd233b7767094abd05c8a015bef61c7c3756d0e` |
| `thread-store/src/local/read_thread.rs` | `2c613b0f0d117ec7a14ddc0e568d46aaac70a51f` |
| `thread-store/src/local/search_threads.rs` | `7e8401aa036e168c595a4e1aee2018ffcd37a9d1` |
| `thread-store/src/local/paginated_fork.rs` | `0b14091267769ac55b5751e20ddb6798196f2431` |
| `core/src/thread_manager.rs` | `db3d358ecfc98be333d11a9c774c32dc25fe91f6` |
| `core/src/session/mod.rs` | `902d18377cb93e6bc378ebac9fc9b740cce65845` |
| `core/src/session/session.rs` | `62be2d34e26cf3ec143852b186cd17b097e8acdd` |
| `core/src/image_preparation.rs` | `02d15459d464ffaa81c812996afb4d80b1da3035` |
| `thread-store/src/local/archive_thread.rs` | `397ab0f9e26dfcd811310b77ee882c775a28fbb4` |
| `thread-store/src/local/unarchive_thread.rs` | `017d18ad00a3739dbc1188dd4faee35d8957c1c7` |
| `thread-store/src/local/thread_attachments.rs` | `aa200552071f0de3b5394a7aad608291a02dea64` |
| `thread-store/src/local/create_thread.rs` | `e3b7fc8a898b5abbd9f392e738d3ee1e88b9e5a0` |
| `thread-store/src/local/update_thread_metadata.rs` | `8cfb38bf2f2474643fb3592258bec436b9c66c8e` |
| `thread-store/src/local/live_writer.rs` | `6a2d95f2b06813fe823b80cc1e22c68c88e2efc1` |
| `thread-store/src/local/revert_thread.rs` | `5aeebce31ee4bdbc53d261feec0b92be88cdec73` |
| `thread-store/src/local/delete_thread.rs` | `103b12d49fd9caaa6aa31284b6ee0117a09b9213` |
| `thread-store/src/local/rollout_migration/canonicalizer.rs` | `8c446ccc5cd7faf39515fc2055d10a3193dbf51c` |
| `thread-store/src/local/rollout_migration.rs` | `c259a6f034dd9bb45559ea665b798327c3983555` |
| `thread-store/src/local/rollout_migration/startup.rs` | `dbc48ed8c06fecaebf19b7f0e7b69d9985c103b2` |
| `thread-store/src/local/rollout_migration/publish.rs` | `4c0f9656fbcd9b4485abe34ba61efe36fdcedd25` |
| `cli/src/migrate_rollouts.rs` | `f7f695b73260fb661dcb4906e3bf38a805475022` |
| `core/src/session/rollout_reconstruction.rs` | `ab20e5b30ef4a363d12d12ee9d5c1375b985a825` |
| `attachment-store/src/lib.rs` | `99cb4826fa22b8df074aa75f055b988bb1ecd86a` |
| `app-server/src/request_processors/thread_attachments.rs` | `ce19d06db94cf72702d387bf13630431ecfae899` |

## Logical representations and observed call sites

| Representation | Producer or mutation | Consumer | Forward/reverse obligation |
|---|---|---|---|
| Active canonical JSONL under `sessions/` | `RolloutRecorder::new_with_writer_lock`, `record_canonical_items`, `persist`, `flush` in `rollout/src/recorder.rs`; `create_thread.rs`, `live_writer.rs`, `revert_thread.rs`, and `update_thread_metadata.rs::update_rollout_metadata` construct or use locked recorders; `unarchive_thread.rs` moves files here | `RolloutRecorder::load_rollout_items`, `get_rollout_history`; `thread-store/src/local/read_thread.rs`, `search_threads.rs`; `archive_thread.rs` moves files away | Preserve raw ordered items, path-derived rollout ID, stable thread/session IDs and acknowledgement boundary; portable path mapping is unresolved |
| Archived canonical JSONL under `archived_sessions/` | `thread-store/src/local/archive_thread.rs` moves files here and updates SQLite metadata | `rollout/src/list.rs`, `RolloutReferenceIndex::scan`, local read/search; `unarchive_thread.rs` moves files back and calls `mark_unarchived` | Preserve archive status and contents together; archive is not deletion |
| Cold `.jsonl.zst` sibling | `rollout/src/compression.rs` compression worker writes and verifies compressed form; `materialize_rollout_for_append` decodes and publishes plain JSONL before deleting compressed source, called by recorder append and `rollout/src/lib.rs::materialize_rollout_for_reference` | `open_rollout_line_reader`, `existing_rollout_path`, recorder load, seekable reader; `thread-store/src/local/rollout_lineage.rs` may materialize before reference use | Capture exactly one authoritative representation and verify decoded bytes; both directions, temporary files and duplicate siblings need reconciliation |
| Reference-backed fork history | `core/src/thread_manager.rs::fork_prepared_thread`; `thread-store/src/local/paginated_fork.rs::prepare` returns `HistoryPosition`, then `create_thread.rs` passes it to the recorder as `SessionMeta.history_base` | `thread-store/src/local/rollout_lineage.rs`, `model_context.rs`; `RolloutReferenceIndex::scan` counts direct references | Preserve ancestor rollout IDs and exclusive ordinals, all referenced ancestors and truncation boundaries; copying only the child's file loses history |
| Copied legacy fork and deferred copied child fork | `core/src/thread_manager.rs` uses `ForkPersistence::Copied` for legacy forks/resume and `CopiedDeferred` in `fork_thread_with_source`; `core/src/session/mod.rs` maps the latter to the subagent-spawn durability barrier; canonicalizer in `thread-store/src/local/rollout_migration/` may rewrite copied history | legacy resume/fork paths in `core/src/thread_manager.rs`; local lineage/read paths | Do not infer a `history_base` graph from copied payloads; source and migrated representation need equivalence proof |
| Paginated thread-history projection in `thread_history_1.sqlite` | `thread-store/src/local/thread_history_materialization.rs::materialize_to_sqlite` reads canonical lineage | `thread-store/src/local/thread_history.rs` dispatches `list_turns`, `list_items`, `list_timeline`, and `search_thread_occurrences` to its read/search/realtime modules | Treat projection as rebuildable only after canonical closure and public reconstruction are proven; see `SOURCE_CATALOG.md` |
| `session_index.jsonl` name index | `rollout/src/session_index.rs::append_thread_name`, `append_session_index_entry`, `remove_thread_name_entries` | `find_thread_name_by_id`, `find_thread_names_by_ids`, `find_thread_meta_by_name_str`, `find_thread_meta_candidates_by_name_str`; local thread resolution | Preserve or rebuild names with the exact duplicate/clear semantics in `SESSION_INDEX_AUDIT.md` |
| Thread attachment metadata/payload | `thread-store/src/local/thread_attachments.rs::{add_thread_attachment,copy_thread_attachments,remove_thread_attachment}` through `codex-state` primary `thread_attachments` | `list_thread_attachments` and `app-server/src/request_processors/thread_attachments.rs` add/list/remove RPC handlers | Metadata row alone does not prove referenced content closure; catalogued with primary state in `SOURCE_CATALOG.md` |
| Image attachment bytes in model items | `core/src/image_preparation.rs` calls `AttachmentStore::upload`; default `InlineAttachmentStore` returns the original bytes, alternate implementation may return `File { file_id }` | Model input preparation passes existing `ImageReference::File` IDs through; the `AttachmentStore::resolve` trait method has no production caller at this revision | Preserve inline bytes or implement resolution and materialization of file references with digest/size/provenance; file IDs and URLs are not portable bytes |

`thread-store/src/local/revert_thread.rs` can rewrite a thread's effective
history boundary, while `delete_thread.rs` checks reference descendants before
removing owned rollout paths. `archive_thread.rs` and `unarchive_thread.rs`
move files between active and archived collections and then update state DB
archive metadata; `compression.rs` may replace a cold JSONL with a
compressed sibling or materialize it back to JSONL. Startup/background
`rollout_migration.rs` and `migrate-rollouts` rewrite canonical history through
`rollout_migration/publish.rs` while rebuilding its SQLite projection. A capture
must fence these mutations together with the live writer, rather than treating
a directory walk as a transaction.

`history/src/lib.rs::RolloutItem` and `history/src/rollout_payload.rs` define the
wire item set: session metadata, response items (including optional harness
metadata), inter-agent communication and its metadata, compaction, turn context,
token usage, world state, security score, retained context, event messages and
realtime items. Unknown/future variants cannot be silently discarded. Session
metadata includes stable `SessionMeta::session_id` and `SessionMeta::id` (thread
ID), history mode, and optional reference coordinates in
`protocol/src/protocol.rs`. A reverted rollout's ID can differ from its thread
ID and is derived from the rollout filename; `HistoryPosition::thread_id`
historically names a rollout ID despite the field name.

For paginated model-visible context, `thread-store/src/local/model_context.rs`
scans the resolved lineage using `rollout/src/model_context.rs::ModelContextScan`.
Its newest compaction permits a bounded suffix only when replacement history
and window number are both present. For legacy history, core's
`session/rollout_reconstruction.rs::select_input_compaction` additionally
requires `resume_metadata`; otherwise full replay is required. Core resume/fork in
`core/src/thread_manager.rs` consumes those results and can also accept a caller
supplied history or path. These are distinct inputs for migration qualification.

## Existing native evidence and unresolved work

Existing tests include `rollout/src/compression_tests.rs`,
`rollout_reference_index_tests.rs`, `seekable_reader_tests.rs`,
`session_index_tests.rs`, `thread-store/src/local/rollout_lineage_tests.rs`,
`model_context_tests.rs`, `thread_history_materialization_tests.rs`, and
`paginated_fork.rs` callers' tests. They test local behavior, not a fresh second
host, a PostgreSQL backend, or source/destination equivalence of outbound model
input. No new test was added for statically defined file names or enum variants.

The `codex-thread-store` fixture
`replays_nested_archived_lineage_from_detached_home` now copies only three
canonical rollout files to a second disposable Codex home: an archived root,
an archived compressed middle, and an active compressed reference-backed child.
It removes the source home before loading the child's model context from the
second home, compares ordered items against the original frozen-prefix replay,
then removes the referenced ancestor and requires a read error. This qualifies
that specific native file-closure fixture without a source SQLite projection.
Embedded source cwd/path values remain unchanged, and the test does not qualify
path relocation, complete capture enumeration, foreign-host execution, live
writer fencing, PostgreSQL, or outbound inference equivalence.

The separate native `stale_sqlite_path_cannot_substitute_another_threads_history`
fixture gives a thread an existing SQLite `rollout_path` that points to a
different thread's canonical file. A history read must return the requested
thread's full file history through ID resolution; after that rightful file is
removed, the same read must fail instead of returning the other thread's
history. This checks one local identity guard under inconsistent metadata.
It does not prove a coherent capture, remap source paths, or qualify another
host or storage backend.

Native archive/unarchive collision fixtures exercise occupied canonical target
paths in both directions. A same-volume atomic no-replace rename refuses an
occupied target without requiring hard-link support or publishing a second name
before a separate source unlink. The fixtures preserve distinct byte streams and
the SQLite metadata row on refusal. Callers hold lifecycle and cross-process
writer locks, but a process bypassing those locks can replace a source, and a
crash after rename before the metadata update can leave stale SQLite path/status.
Multi-file rollback and durable ownership-checked recovery remain separate
requirements; these fixtures do not establish a cross-store transaction.
Cross-volume moves fail without copy fallback and preserve the source.

The remaining #2/#11 inventory must trace every app-server/TUI/exec/daemon read
entry point, file-backed attachment implementation, ephemeral sessions (which
have no durable rollout to capture), caller-supplied history, memory files,
compaction/fork-only model items, and
provider/workspace permissions. It must determine which live writer owns each
file during coherent capture, how a reference graph closes across active,
archived and compressed files, and how a fresh host materializes every byte.
The offline bundle verifier remains `activation_permitted=false`.
