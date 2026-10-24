//! Atomic same-volume rollout name transfer without replacing the destination.
//!
//! Callers hold the lifecycle and cross-process writer locks. Like ordinary rename, this
//! does not protect against processes bypassing those locks or make SQLite changes atomic.

use std::io;
use std::path::Path;

pub(super) fn rename_noclobber(source: &Path, destination: &Path) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let source = CString::new(source.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in source path"))?;
        let destination = CString::new(destination.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in destination path"))?;
        // SAFETY: SYS_renameat2 uses two directory descriptors, two C string pointers
        // and an integer flag. Both NUL-terminated paths remain alive for the call;
        // integer arguments use syscall's word-sized ABI on GNU and musl alike.
        if unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD as libc::c_long,
                source.as_ptr(),
                libc::AT_FDCWD as libc::c_long,
                destination.as_ptr(),
                libc::RENAME_NOREPLACE as libc::c_long,
            )
        } == 0
        {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(target_os = "macos")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let source = CString::new(source.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in source path"))?;
        let destination = CString::new(destination.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in destination path"))?;
        // SAFETY: Both C strings are NUL terminated and remain alive for the call.
        if unsafe {
            libc::renameatx_np(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_EXCL,
            )
        } == 0
        {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::MoveFileExW;

        let source: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
        let destination: Vec<u16> = destination
            .as_os_str()
            .encode_wide()
            .chain(Some(0))
            .collect();
        // SAFETY: Both UTF-16 paths are NUL terminated and remain alive for the call. Passing
        // zero flags forbids replacement and cross-volume copy fallback.
        if unsafe { MoveFileExW(source.as_ptr(), destination.as_ptr(), /*dwflags*/ 0) } != 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = (source, destination);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "atomic no-replace rename is unavailable on this platform",
        ))
    }
}

#[cfg(test)]
#[path = "rollout_move_noclobber_rename_tests.rs"]
mod tests;
