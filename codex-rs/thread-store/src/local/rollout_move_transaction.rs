//! Archive/unarchive intent replayed after a process crash, before normal path discovery.
//! Unix syncs the journal's directory entries. Windows does not currently promise
//! power-loss durability for renamed directory entries.

use std::io;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use chrono::Utc;
use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;

use super::LocalThreadStore;
use super::rollout_move_file::clear_rollout_move_intent;
use super::rollout_move_file::move_rollout_noclobber_retained_bound;
use super::rollout_move_file::published_rollout_move_owned;
use super::rollout_move_file::published_rollout_source_binding;
use super::rollout_move_file::touch_modified_time;
use super::rollout_move_file::verify_published_rollout_move;
use super::rollout_move_identity::RolloutFileIdentity;
use super::rollout_move_identity::rollout_file_digest;
use super::rollout_move_identity::rollout_file_identity;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) enum MoveDirection {
    Archive,
    Unarchive,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct MovePair {
    source: PathBuf,
    destination: PathBuf,
    source_id: RolloutFileIdentity,
    source_digest: [u8; 32],
}

// Journals written before source binding recorded only the paths. Keep this exact
// disk shape readable across upgrades; replay binds it before any move is made.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyMovePair {
    source: PathBuf,
    destination: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyMoveTransaction {
    thread_id: ThreadId,
    direction: MoveDirection,
    selected_destination: PathBuf,
    moves: Vec<LegacyMovePair>,
}

#[derive(Debug, Deserialize, Serialize)]
struct MoveTransaction {
    thread_id: ThreadId,
    direction: MoveDirection,
    selected_destination: PathBuf,
    moves: Vec<MovePair>,
}

pub(super) struct PendingMove {
    path: PathBuf,
    transaction: MoveTransaction,
}

pub(super) fn begin_move(
    codex_home: &Path,
    thread_id: ThreadId,
    direction: MoveDirection,
    selected_destination: &Path,
    moves: &[(PathBuf, PathBuf)],
) -> io::Result<PendingMove> {
    if moves.is_empty()
        || !moves
            .iter()
            .any(|(_, destination)| destination == selected_destination)
    {
        return Err(io::Error::other(
            "selected rollout is missing from move plan",
        ));
    }
    let transaction = MoveTransaction {
        thread_id,
        direction,
        selected_destination: selected_destination.to_path_buf(),
        moves: moves
            .iter()
            .map(|(source, destination)| {
                Ok(MovePair {
                    source: source.clone(),
                    destination: destination.clone(),
                    source_id: rollout_file_identity(source)?,
                    source_digest: rollout_file_digest(source)?,
                })
            })
            .collect::<io::Result<Vec<_>>>()?,
    };
    let path = transaction_path(codex_home, thread_id);
    match std::fs::symlink_metadata(&path) {
        Ok(_) => return Err(io::Error::from(io::ErrorKind::AlreadyExists)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    for pair in &transaction.moves {
        validate_move_pair(codex_home, pair)?;
        verify_planned_source(pair)?;
        // Reject a known collision before publishing the journal. A failed archive must not
        // leave an intent that blocks ordinary reads of an intact source.
        match std::fs::symlink_metadata(&pair.destination) {
            Ok(_) => return Err(io::Error::from(io::ErrorKind::AlreadyExists)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        if pair.source.exists() && !pair.destination.exists() {
            // A prior committed transaction can leave a sidecar if cleanup was interrupted
            // after its transaction file was removed. No destination was published here.
            match clear_rollout_move_intent(&pair.destination) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        }
    }
    let journal_parent = path
        .parent()
        .ok_or_else(|| io::Error::other("missing journal parent"))?;
    std::fs::create_dir_all(journal_parent)?;
    #[cfg(unix)]
    std::fs::File::open(codex_home)?.sync_all()?;
    if !std::fs::canonicalize(journal_parent)?.starts_with(std::fs::canonicalize(codex_home)?) {
        return Err(io::Error::other("rollout move journal escapes Codex home"));
    }
    let mut file = tempfile::Builder::new()
        .prefix(".codex-move-transaction-")
        .tempfile_in(journal_parent)?;
    serde_json::to_writer(&mut file, &transaction).map_err(io::Error::other)?;
    file.write_all(b"\n")?;
    file.as_file().sync_all()?;
    file.persist_noclobber(&path).map_err(|err| err.error)?;
    #[cfg(unix)]
    std::fs::File::open(
        path.parent()
            .ok_or_else(|| io::Error::other("missing journal parent"))?,
    )?
    .sync_all()?;
    Ok(PendingMove { path, transaction })
}

impl PendingMove {
    pub(super) fn move_all(&self, codex_home: &Path) -> io::Result<()> {
        // A later replacement must not leave an earlier file published and quarantined.
        for pair in &self.transaction.moves {
            verify_planned_source(pair)?;
        }
        for pair in &self.transaction.moves {
            move_rollout_noclobber_retained_bound(
                &pair.source,
                &pair.destination,
                codex_home,
                pair.source_id,
                pair.source_digest,
            )?;
        }
        Ok(())
    }

    pub(super) fn complete(self) -> io::Result<()> {
        // SQLite may already reflect the move if the process exits here. Replay checks
        // the row and skips an idempotent write before removing this journal.
        self.cleanup()
    }

    fn abandon_unmoved(self) -> io::Result<()> {
        std::fs::remove_file(&self.path)?;
        #[cfg(unix)]
        std::fs::File::open(
            self.path
                .parent()
                .ok_or_else(|| io::Error::other("missing journal parent"))?,
        )?
        .sync_all()?;
        Ok(())
    }

    fn cleanup(self) -> io::Result<()> {
        // Remove the transaction first. If cleanup stops here, the SQLite update is committed
        // and the remaining sidecars are harmless; begin_move clears them before reuse.
        std::fs::remove_file(&self.path)?;
        #[cfg(unix)]
        std::fs::File::open(
            self.path
                .parent()
                .ok_or_else(|| io::Error::other("missing journal parent"))?,
        )?
        .sync_all()?;
        for pair in &self.transaction.moves {
            match clear_rollout_move_intent(&pair.destination) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => tracing::warn!(
                    "failed to remove committed rollout move receipt `{}`: {err}",
                    pair.destination.display()
                ),
            }
        }
        Ok(())
    }
}

pub(super) async fn replay_pending_move(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<Option<MoveDirection>> {
    let Some((pending, committed)) = load_move(store.config.codex_home.as_path(), thread_id)
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to read pending rollout move: {err}"),
        })?
    else {
        return Ok(None);
    };
    let direction = pending.transaction.direction;
    let home = store.config.codex_home.as_path();
    if pending.transaction.moves.is_empty()
        || !pending
            .transaction
            .moves
            .iter()
            .any(|pair| pair.destination == pending.transaction.selected_destination)
    {
        return Err(ThreadStoreError::Internal {
            message: "pending rollout move has an invalid selected destination".to_string(),
        });
    }
    for pair in &pending.transaction.moves {
        validate_move_pair(home, pair).map_err(|err| ThreadStoreError::Internal {
            message: format!("pending rollout move path is invalid: {err}"),
        })?;
        // Validate every surviving source before either abandoning a collision or moving
        // another file. A journal is authority for the original files, not replacements.
        match std::fs::symlink_metadata(&pair.source) {
            Ok(_) => verify_planned_source(pair).map_err(|err| ThreadStoreError::Internal {
                message: format!("pending rollout source changed: {err}"),
            })?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(ThreadStoreError::Internal {
                    message: format!("failed to inspect pending rollout source: {err}"),
                });
            }
        }
    }
    if !committed
        && pending.transaction.moves.iter().all(|pair| {
            std::fs::symlink_metadata(&pair.source).is_ok_and(|m| m.file_type().is_file())
        })
    {
        let mut owned_destination = false;
        let mut collided_destination = false;
        for pair in &pending.transaction.moves {
            match std::fs::symlink_metadata(&pair.destination) {
                Ok(_) => {
                    if published_rollout_move_owned(
                        &pair.source,
                        &pair.destination,
                        pair.source_id,
                        pair.source_digest,
                    )
                    .map_err(|err| ThreadStoreError::Internal {
                        message: format!("failed to inspect pending rollout publication: {err}"),
                    })? {
                        owned_destination = true;
                    } else {
                        collided_destination = true;
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(ThreadStoreError::Internal {
                        message: format!("failed to inspect pending rollout destination: {err}"),
                    });
                }
            }
        }
        if collided_destination && !owned_destination {
            pending
                .abandon_unmoved()
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to abandon unstarted rollout move: {err}"),
                })?;
            return Ok(None);
        }
    }
    if !committed {
        // Validate every recorded file before mutating any. The journal may have been damaged
        // or externally changed, and a destination-only file must have a published receipt.
        for pair in &pending.transaction.moves {
            if std::fs::symlink_metadata(&pair.destination).is_ok()
                && !published_rollout_move_owned(
                    &pair.source,
                    &pair.destination,
                    pair.source_id,
                    pair.source_digest,
                )
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to verify pending rollout ownership: {err}"),
                })?
            {
                return Err(ThreadStoreError::Internal {
                    message: "pending rollout destination is not owned by this move".to_string(),
                });
            }
            let readable = if pair.source.exists() {
                pair.source.as_path()
            } else {
                verify_published_rollout_move(&pair.source, &pair.destination, home).map_err(
                    |err| ThreadStoreError::Internal {
                        message: format!("failed to verify completed rollout move: {err}"),
                    },
                )?;
                pair.destination.as_path()
            };
            if rollout_file_digest(readable).map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to hash pending rollout: {err}"),
            })? != pair.source_digest
            {
                return Err(ThreadStoreError::Internal {
                    message: "pending rollout content differs from journaled source".to_string(),
                });
            }
            let session_meta = codex_rollout::read_session_meta_line(readable)
                .await
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to validate pending rollout owner: {err}"),
                })?;
            if session_meta.meta.id != thread_id {
                return Err(ThreadStoreError::Internal {
                    message: "pending rollout move belongs to another thread".to_string(),
                });
            }
        }
        for pair in &pending.transaction.moves {
            if pair.source.exists() {
                move_rollout_noclobber_retained_bound(
                    &pair.source,
                    &pair.destination,
                    home,
                    pair.source_id,
                    pair.source_digest,
                )
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to replay rollout move: {err}"),
                })?;
            }
        }
        let selected = pending.transaction.selected_destination.as_path();
        if direction == MoveDirection::Unarchive {
            touch_modified_time(selected).map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to touch restored rollout: {err}"),
            })?;
        }
        if let Some(ctx) = store.state_db().await {
            let metadata =
                ctx.get_thread(thread_id)
                    .await
                    .map_err(|err| ThreadStoreError::Internal {
                        message: format!("failed to read rollout metadata during replay: {err}"),
                    })?;
            let already_updated = metadata.as_ref().is_some_and(|metadata| {
                metadata.rollout_path == selected
                    && match direction {
                        MoveDirection::Archive => metadata.archived_at.is_some(),
                        MoveDirection::Unarchive => metadata.archived_at.is_none(),
                    }
            });
            if !already_updated {
                match direction {
                    MoveDirection::Archive => {
                        ctx.mark_archived(thread_id, selected, Utc::now()).await
                    }
                    MoveDirection::Unarchive => ctx.mark_unarchived(thread_id, selected).await,
                }
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to replay rollout metadata: {err}"),
                })?;
            }
        }
        pending
            .complete()
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to complete replayed rollout move: {err}"),
            })?;
    } else {
        if let Some(ctx) = store.state_db().await
            && let Some(metadata) =
                ctx.get_thread(thread_id)
                    .await
                    .map_err(|err| ThreadStoreError::Internal {
                        message: format!("failed to verify committed rollout metadata: {err}"),
                    })?
        {
            let archive_state_matches = match direction {
                MoveDirection::Archive => metadata.archived_at.is_some(),
                MoveDirection::Unarchive => metadata.archived_at.is_none(),
            };
            if metadata.rollout_path != pending.transaction.selected_destination
                || !archive_state_matches
            {
                return Err(ThreadStoreError::Internal {
                    message: "committed rollout move disagrees with SQLite".to_string(),
                });
            }
        }
        for pair in &pending.transaction.moves {
            if !published_rollout_move_owned(
                &pair.source,
                &pair.destination,
                pair.source_id,
                pair.source_digest,
            )
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to verify committed rollout ownership: {err}"),
            })? {
                return Err(ThreadStoreError::Internal {
                    message: "committed rollout destination is not owned by this move".to_string(),
                });
            }
            verify_published_rollout_move(&pair.source, &pair.destination, home).map_err(
                |err| ThreadStoreError::Internal {
                    message: format!("failed to verify committed rollout move: {err}"),
                },
            )?;
            let session_meta = codex_rollout::read_session_meta_line(&pair.destination)
                .await
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to validate committed rollout owner: {err}"),
                })?;
            if session_meta.meta.id != thread_id {
                return Err(ThreadStoreError::Internal {
                    message: "committed rollout move belongs to another thread".to_string(),
                });
            }
        }
        pending
            .cleanup()
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to clean committed rollout move: {err}"),
            })?;
    }
    Ok(Some(direction))
}

fn validate_move_pair(codex_home: &Path, pair: &MovePair) -> io::Result<()> {
    validate_move_paths(codex_home, &pair.source, &pair.destination)
}

fn validate_move_paths(codex_home: &Path, source: &Path, destination: &Path) -> io::Result<()> {
    let sessions = std::fs::canonicalize(codex_home.join(codex_rollout::SESSIONS_SUBDIR))?;
    let archived = std::fs::canonicalize(codex_home.join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR))?;
    let source_parent = std::fs::canonicalize(
        source
            .parent()
            .ok_or_else(|| io::Error::other("rollout source has no parent"))?,
    )?;
    let destination_parent = std::fs::canonicalize(
        destination
            .parent()
            .ok_or_else(|| io::Error::other("rollout destination has no parent"))?,
    )?;
    let same_filename = source.file_name() == destination.file_name()
        && codex_rollout::rollout_id_from_path(source).is_some();
    let opposite_collections = (source_parent.starts_with(&sessions)
        && destination_parent.starts_with(&archived))
        || (source_parent.starts_with(&archived) && destination_parent.starts_with(&sessions));
    if !same_filename || !opposite_collections {
        return Err(io::Error::other("rollout move is outside its collection"));
    }
    Ok(())
}

fn verify_planned_source(pair: &MovePair) -> io::Result<()> {
    if rollout_file_identity(&pair.source)? != pair.source_id
        || rollout_file_digest(&pair.source)? != pair.source_digest
    {
        return Err(io::Error::other(
            "rollout source identity or content differs from journal",
        ));
    }
    Ok(())
}

fn transaction_path(codex_home: &Path, thread_id: ThreadId) -> PathBuf {
    codex_home
        .join("rollout_move_transactions")
        .join(format!("{thread_id}.json"))
}

pub(super) fn pending_move_exists(codex_home: &Path, thread_id: ThreadId) -> io::Result<bool> {
    match std::fs::symlink_metadata(transaction_path(codex_home, thread_id)) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

fn load_move(codex_home: &Path, thread_id: ThreadId) -> io::Result<Option<(PendingMove, bool)>> {
    let path = transaction_path(codex_home, thread_id);
    let Some(parent) = path.parent() else {
        return Err(io::Error::other("missing journal parent"));
    };
    match std::fs::canonicalize(parent) {
        Ok(canonical_parent)
            if canonical_parent.starts_with(std::fs::canonicalize(codex_home)?) => {}
        Ok(_) => return Err(io::Error::other("rollout move journal escapes Codex home")),
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    }
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => {
            return Err(io::Error::other(
                "rollout move journal is not a regular file",
            ));
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    }
    let mut file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    if file.metadata()?.len() > 1_048_576 {
        return Err(io::Error::other("pending rollout move is too large"));
    }
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    if !contents.ends_with('\n') {
        return Err(io::Error::other("pending rollout move is incomplete"));
    }
    let mut lines = contents.lines();
    let record = lines.next().ok_or_else(|| io::Error::other("empty move"))?;
    let transaction: MoveTransaction = match serde_json::from_str(record) {
        Ok(transaction) => transaction,
        Err(_) => {
            let legacy: LegacyMoveTransaction =
                serde_json::from_str(record).map_err(io::Error::other)?;
            if legacy.thread_id != thread_id {
                return Err(io::Error::other("pending rollout move thread ID mismatch"));
            }
            let moves = legacy
                .moves
                .into_iter()
                .map(|pair| {
                    validate_move_paths(codex_home, &pair.source, &pair.destination)?;
                    let (source_id, source_digest) = match std::fs::symlink_metadata(&pair.source) {
                        Ok(metadata) if metadata.file_type().is_file() => (
                            rollout_file_identity(&pair.source)?,
                            rollout_file_digest(&pair.source)?,
                        ),
                        Ok(_) => {
                            return Err(io::Error::other("legacy rollout source is not a file"));
                        }
                        Err(err) if err.kind() == io::ErrorKind::NotFound => {
                            published_rollout_source_binding(&pair.source, &pair.destination)?
                        }
                        Err(err) => return Err(err),
                    };
                    Ok(MovePair {
                        source: pair.source,
                        destination: pair.destination,
                        source_id,
                        source_digest,
                    })
                })
                .collect::<io::Result<Vec<_>>>()?;
            MoveTransaction {
                thread_id: legacy.thread_id,
                direction: legacy.direction,
                selected_destination: legacy.selected_destination,
                moves,
            }
        }
    };
    if transaction.thread_id != thread_id {
        return Err(io::Error::other("pending rollout move thread ID mismatch"));
    }
    let committed = match lines.next() {
        None => false,
        Some("committed") => true,
        Some(_) => return Err(io::Error::other("invalid pending rollout move status")),
    };
    if lines.next().is_some() {
        return Err(io::Error::other("pending rollout move has extra records"));
    }
    Ok(Some((PendingMove { path, transaction }, committed)))
}

#[cfg(test)]
#[path = "rollout_move_transaction_tests.rs"]
mod tests;
