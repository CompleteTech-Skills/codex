use std::io;

use pretty_assertions::assert_eq;

use super::rename_noclobber;

#[test]
fn moves_file_without_replacing_an_existing_target() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source");
    let destination = directory.path().join("destination");
    std::fs::write(&source, b"owned")?;
    std::fs::write(&destination, b"unrelated")?;

    let error = rename_noclobber(&source, &destination)
        .expect_err("an occupied destination must remain untouched");
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&source)?, b"owned");
    assert_eq!(std::fs::read(&destination)?, b"unrelated");

    std::fs::remove_file(&destination)?;
    rename_noclobber(&source, &destination)?;
    assert!(!source.exists());
    assert_eq!(std::fs::read(&destination)?, b"owned");
    Ok(())
}

#[test]
fn does_not_replace_an_existing_directory() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source");
    let destination = directory.path().join("destination");
    std::fs::write(&source, b"owned")?;
    std::fs::create_dir(&destination)?;

    assert!(rename_noclobber(&source, &destination).is_err());
    assert_eq!(std::fs::read(&source)?, b"owned");
    assert!(destination.is_dir());
    Ok(())
}

#[cfg(unix)]
#[test]
fn cross_filesystem_move_never_copies_or_removes_source() -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let source_directory = tempfile::tempdir_in(std::env::current_dir()?)?;
    let destination_directory = tempfile::tempdir()?;
    if source_directory.path().metadata()?.dev() == destination_directory.path().metadata()?.dev() {
        // Most CI hosts put the checkout and temporary files on the same filesystem.
        return Ok(());
    }
    let source = source_directory.path().join("source");
    let destination = destination_directory.path().join("destination");
    std::fs::write(&source, b"owned")?;

    let error = rename_noclobber(&source, &destination)
        .expect_err("a cross-filesystem move must not fall back to copying");
    assert_eq!(error.raw_os_error(), Some(libc::EXDEV));
    assert_eq!(std::fs::read(&source)?, b"owned");
    assert!(!destination.exists());
    Ok(())
}

#[test]
fn competing_moves_preserve_the_losing_source() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let sources = [directory.path().join("first"), directory.path().join("second")];
    let destination = directory.path().join("destination");
    std::fs::write(&sources[0], b"first")?;
    std::fs::write(&sources[1], b"second")?;
    let barrier = std::sync::Barrier::new(/*n*/ 2);
    let outcomes = std::thread::scope(|scope| {
        let handles: Vec<_> = sources.iter().map(|source| {
            let barrier = &barrier;
            let destination = &destination;
            scope.spawn(move || {
                barrier.wait();
                rename_noclobber(source, destination)
            })
        }).collect();
        handles.into_iter().map(|handle| handle.join().expect("move worker")).collect::<Vec<_>>()
    });
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    let winner = outcomes.iter().position(Result::is_ok).expect("one winner");
    let loser = 1 - winner;
    assert_eq!(outcomes[loser].as_ref().expect_err("one collision").kind(), io::ErrorKind::AlreadyExists);
    let bytes = [b"first".as_slice(), b"second".as_slice()];
    assert_eq!(std::fs::read(&destination)?, bytes[winner]);
    assert_eq!(std::fs::read(&sources[loser])?, bytes[loser]);
    assert!(!sources[winner].exists());
    Ok(())
}
