//! Bounded physical compressed-header reader for offline snapshot qualification.

use std::fs;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::ExitCode;

use codex_rollout::RolloutItem;
use serde_json::json;

const MAX_REQUESTS: usize = 1024;
const MAX_REQUEST_LINE: usize = 4096;
const MAX_BATCH_BYTES: u64 = (MAX_REQUESTS * (MAX_REQUEST_LINE + 1)) as u64;
// This caps compressed bytes consumed while finding a header, not total rollout file size.
const MAX_COMPRESSED_BYTES: u64 = 1024 * 1024;
const MAX_DECODED_BYTES: u64 = 64 * 1024;
const MAX_WINDOW_LOG: u32 = 23;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    relative_path: String,
}

#[cfg(test)]
#[path = "codex_rollout_header_preview/tests.rs"]
mod tests;

fn main() -> ExitCode {
    let mut args = std::env::args_os();
    let valid_flag = args.next().is_some()
        && args.next().as_deref() == Some(std::ffi::OsStr::new("--snapshot-home"));
    let root = args.next();
    let root = match (valid_flag, root) {
        (true, Some(root)) if args.next().is_none() => root,
        _ => {
            println!(
                "{}",
                json!({"status": "rejected", "code": "invalid_request"})
            );
            return ExitCode::from(2);
        }
    };
    let mut output = io::stdout().lock();
    match run(Path::new(&root), io::stdin().lock(), &mut output) {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => {
            let _ = writeln!(output, "{}", json!({"status": "rejected", "code": code}));
            ExitCode::from(2)
        }
    }
}

fn run(root: &Path, input: impl Read, output: &mut impl Write) -> Result<(), &'static str> {
    let mut batch = Vec::new();
    input
        .take(MAX_BATCH_BYTES + 1)
        .read_to_end(&mut batch)
        .map_err(|_| "invalid_request")?;
    if batch.len() as u64 > MAX_BATCH_BYTES {
        return Err("request_limit");
    }
    if !root.is_absolute()
        || !checked_metadata(root)
            .map_err(|_| "input_unavailable")?
            .is_some_and(|metadata| metadata.is_dir())
    {
        return Err("input_unavailable");
    }
    if !batch.is_empty() && batch.last() != Some(&b'\n') {
        return Err("invalid_request");
    }
    let mut paths = Vec::new();
    if !batch.is_empty() {
        for line in batch[..batch.len() - 1].split(|byte| *byte == b'\n') {
            if line.is_empty() {
                return Err("invalid_request");
            }
            if line.len() > MAX_REQUEST_LINE || paths.len() == MAX_REQUESTS {
                return Err("request_limit");
            }
            let request: Request = serde_json::from_slice(line).map_err(|_| "invalid_request")?;
            paths.push(checked_relative(root, &request.relative_path).ok_or("invalid_request")?);
        }
    }
    // No per-file result is emitted until the entire bounded batch is valid.
    for path in paths {
        let result = match read_header(&path) {
            Ok((thread_id, ancestor)) => json!({
                "status": "ok", "thread_id": thread_id, "ancestor_rollout_id": ancestor,
            }),
            Err(code) => json!({"status": "unresolved", "code": code}),
        };
        writeln!(output, "{result}").map_err(|_| "output_unavailable")?;
    }
    Ok(())
}

fn checked_relative(root: &Path, relative: &str) -> Option<PathBuf> {
    if relative.contains(&['\\', ':', '\0'][..]) {
        return None;
    }
    let parts: Vec<_> = relative.split('/').collect();
    if parts
        .iter()
        .any(|part| part.is_empty() || *part == "." || *part == "..")
    {
        return None;
    }
    let file_name = *parts.last()?;
    if !file_name.ends_with(".jsonl.zst")
        || codex_rollout::rollout_id_from_path(Path::new(file_name)).is_none()
    {
        return None;
    }
    match parts.as_slice() {
        ["archived_sessions", _] => {}
        ["sessions", year, month, day, _] => {
            let (expected_year, expected_month, expected_day) =
                codex_rollout::rollout_date_parts(std::ffi::OsStr::new(file_name))?;
            if (*year, *month, *day)
                != (
                    expected_year.as_str(),
                    expected_month.as_str(),
                    expected_day.as_str(),
                )
            {
                return None;
            }
        }
        _ => return None,
    }
    Some(
        parts
            .into_iter()
            .fold(root.to_path_buf(), |path, part| path.join(part)),
    )
}

fn checked_metadata(path: &Path) -> io::Result<Option<fs::Metadata>> {
    for component in path.ancestors() {
        let metadata = fs::symlink_metadata(component)?;
        if metadata.file_type().is_symlink() || is_windows_reparse_point(&metadata) {
            return Ok(None);
        }
    }
    Ok(Some(fs::symlink_metadata(path)?))
}

#[cfg(windows)]
fn is_windows_reparse_point(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_windows_reparse_point(_metadata: &fs::Metadata) -> bool {
    false
}

fn read_header(path: &Path) -> Result<(String, Option<String>), &'static str> {
    let metadata = checked_metadata(path)
        .map_err(|_| "io_error")?
        .ok_or("symlink")?;
    if !metadata.is_file() {
        return Err("not_regular");
    }
    let compressed_too_large = metadata.len() > MAX_COMPRESSED_BYTES;
    let source = File::open(path).map_err(|_| "io_error")?;
    let mut decoder = zstd::stream::read::Decoder::new(source.take(MAX_COMPRESSED_BYTES))
        .map_err(|_| "decode_error")?;
    decoder
        .window_log_max(MAX_WINDOW_LOG)
        .map_err(|_| "decode_error")?;
    let mut reader = io::BufReader::new(decoder.take(MAX_DECODED_BYTES + 1));
    let mut line = Vec::new();
    loop {
        line.clear();
        let count = match reader.read_until(b'\n', &mut line) {
            Ok(count) => count,
            Err(_) if reader.get_ref().get_ref().get_ref().get_ref().limit() == 0 => {
                return Err("compressed_limit");
            }
            Err(_) => return Err("decode_error"),
        };
        if reader.get_ref().limit() == 0 {
            return Err("decoded_limit");
        }
        if count == 0 {
            return Err(if compressed_too_large {
                "compressed_limit"
            } else {
                "missing_metadata"
            });
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let record =
            codex_rollout::parse_rollout_line_bytes(&line).map_err(|_| "invalid_metadata")?;
        match record.item {
            RolloutItem::SessionMeta(meta) => {
                return Ok((
                    meta.meta.id.to_string(),
                    meta.meta
                        .history_base
                        .map(|base| base.thread_id.to_string()),
                ));
            }
            _ => return Err("invalid_metadata"),
        }
    }
}
