// Copyright (C) 2026 Ammanuel Getenet
// SPDX-License-Identifier: GPL-3.0-or-later
// This file is part of Lemon Studio: https://github.com/ItsAlphaNeon/Lemon-Studio

//! Project files are replaced only after their complete contents have reached a temporary file on the same volume.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

pub fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    write_with(path, |file| file.write_all(bytes))
}

fn write_with(path: &Path, write: impl FnOnce(&mut File) -> io::Result<()>) -> io::Result<()> {
    // Preserve the old behaviour for a user-chosen symlink, replacing its target rather than the link itself.
    let resolved;
    let path = if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
        resolved = path.canonicalize()?;
        resolved.as_path()
    } else {
        path
    };
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let permissions = match std::fs::metadata(path) {
        Ok(meta) => {
            if meta.permissions().readonly() {
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, "project file is read-only"));
            }
            // Atomic rename needs directory permissions; retain the original file's write-access requirement too.
            File::options().write(true).open(path)?;
            Some(meta.permissions())
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(e),
    };
    let mut builder = tempfile::Builder::new();
    #[cfg(unix)]
    if permissions.is_none() {
        use std::os::unix::fs::PermissionsExt;
        // Keep std::fs::write's mode for new shared projects; the kernel applies the process umask at creation.
        builder.permissions(std::fs::Permissions::from_mode(0o666));
    }
    let mut temp = builder.tempfile_in(parent)?;
    write(temp.as_file_mut())?;
    if let Some(permissions) = permissions {
        temp.as_file().set_permissions(permissions)?;
    }
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_projects_are_not_replaced_even_when_the_directory_is_writable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("song.flp");
        std::fs::write(&path, b"original").unwrap();
        let original_permissions = std::fs::metadata(&path).unwrap().permissions();
        let mut read_only = original_permissions.clone();
        read_only.set_readonly(true);
        std::fs::set_permissions(&path, read_only).unwrap();
        assert_eq!(
            write_atomic(&path, b"replacement").unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"original");
        std::fs::set_permissions(&path, original_permissions).unwrap();
    }

    #[test]
    fn a_partial_write_does_not_touch_the_original_or_leave_a_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("song.flp");
        std::fs::write(&path, b"original project").unwrap();
        let failed = write_with(&path, |file| {
            file.write_all(b"partial")?;
            Err(io::Error::other("simulated disk full"))
        });
        assert!(failed.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"original project");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        write_atomic(&path, b"complete new project").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"complete new project");
    }

    #[cfg(unix)]
    #[test]
    fn new_projects_follow_the_same_umask_as_normal_file_creation() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let normal = dir.path().join("normal.flp");
        let atomic = dir.path().join("atomic.flp");
        std::fs::write(&normal, b"normal").unwrap();
        write_atomic(&atomic, b"atomic").unwrap();
        assert_eq!(
            std::fs::metadata(normal).unwrap().permissions().mode(),
            std::fs::metadata(atomic).unwrap().permissions().mode()
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_and_existing_file_permissions_survive() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("song.flp");
        let link = dir.path().join("link.flp");
        std::fs::write(&path, b"old").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        symlink(&path, &link).unwrap();
        write_atomic(&link, b"new").unwrap();
        assert!(std::fs::symlink_metadata(&link).unwrap().file_type().is_symlink());
        assert_eq!(std::fs::read(path).unwrap(), b"new");
        assert_eq!(std::fs::metadata(link).unwrap().permissions().mode() & 0o777, 0o640);
    }
}
