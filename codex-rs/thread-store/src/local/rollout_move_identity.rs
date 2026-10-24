use std::fs::File;
use std::io;
use std::path::Path;

use serde::Deserialize;
use serde::Serialize;

/// Stable filesystem identity used to verify a move after its source was unlinked.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) enum RolloutFileIdentity {
    #[cfg(unix)]
    Unix { device: u64, inode: u64 },
    #[cfg(windows)]
    Windows { volume: u64, file_id: [u8; 16] },
}

pub(super) fn rollout_file_identity(path: &Path) -> io::Result<RolloutFileIdentity> {
    if !std::fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(io::Error::other("rollout move path is not a regular file"));
    }
    rollout_file_identity_from_handle(&std::fs::File::open(path)?)
}

pub(super) fn rollout_file_identity_from_handle(file: &File) -> io::Result<RolloutFileIdentity> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let metadata = file.metadata()?;
        Ok(RolloutFileIdentity::Unix {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;

        use windows_sys::Win32::Storage::FileSystem::FILE_ID_INFO;
        use windows_sys::Win32::Storage::FileSystem::FileIdInfo;
        use windows_sys::Win32::Storage::FileSystem::GetFileInformationByHandleEx;

        let mut info: FILE_ID_INFO = unsafe { std::mem::zeroed() };
        let result = unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle() as _,
                FileIdInfo,
                (&mut info as *mut FILE_ID_INFO).cast(),
                std::mem::size_of::<FILE_ID_INFO>() as u32,
            )
        };
        if result == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(RolloutFileIdentity::Windows {
            volume: info.VolumeSerialNumber,
            file_id: info.FileId.Identifier,
        })
    }
}

pub(super) fn rollout_file_digest(path: &Path) -> io::Result<[u8; 32]> {
    if !std::fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(io::Error::other("rollout move path is not a regular file"));
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update_reader(std::fs::File::open(path)?)?;
    Ok(*hasher.finalize().as_bytes())
}

#[cfg(test)]
#[path = "rollout_move_identity_tests.rs"]
mod tests;


pub(super) fn rollout_directory_identity(path: &Path) -> io::Result<RolloutFileIdentity> {
    if !std::fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(io::Error::other("rollout resource is not a directory"));
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        options.custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let file = options.open(path)?;
    if !file.metadata()?.file_type().is_dir() || !std::fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(io::Error::other("rollout directory changed while opening"));
    }
    let identity = rollout_file_identity_from_handle(&file)?;
    let current = options.open(path)?;
    if !current.metadata()?.file_type().is_dir() || rollout_file_identity_from_handle(&current)? != identity {
        return Err(io::Error::other("rollout directory identity changed while opening"));
    }
    Ok(identity)
}
