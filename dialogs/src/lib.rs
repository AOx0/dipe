#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![deny(rust_2018_idioms, unsafe_code)]
#![deny(clippy::missing_errors_doc)]
#![deny(clippy::missing_panics_doc)]
#![deny(clippy::unwrap_used)]

use rfd::{FileDialog, MessageDialog, MessageDialogResult};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy)]
enum OpKind {
    OpenFile,
    OpenFolder,
    SaveFile,
}

#[derive(Debug, Clone, Copy)]
enum MultiOpKind {
    OpenFiles,
    OpenFolders,
}

fn ask_multiple_op(
    kind: MultiOpKind,
    title: &str,
    filters: Option<&[(&str, &[&str])]>,
) -> Option<Vec<PathBuf>> {
    loop {
        let dialog = append_filters(filters, FileDialog::new()).set_title(title);
        let res = match kind {
            MultiOpKind::OpenFiles => dialog.pick_files(),
            MultiOpKind::OpenFolders => dialog.pick_folders(),
        };

        let Some(file) = res else {
            if let MessageDialogResult::Yes = confirm_cancel() {
                break None;
            }

            continue;
        };

        break Some(file);
    }
}

fn ask_single_op(
    kind: OpKind,
    title: &str,
    filters: Option<&[(&str, &[&str])]>,
) -> Option<PathBuf> {
    loop {
        let dialog = append_filters(filters, FileDialog::new()).set_title(title);
        let res = match kind {
            OpKind::OpenFile => dialog.pick_file(),
            OpKind::OpenFolder => dialog.pick_folder(),
            OpKind::SaveFile => dialog.save_file(),
        };

        let Some(file) = res else {
            if let MessageDialogResult::Yes = confirm_cancel() {
                break None;
            }

            continue;
        };

        break Some(file);
    }
}

/// Ask where to save a file. Returns None if the user confirmed he wanted to cancel the selection.
///
/// After the user asking for the operation to be cancelled the program may be terminated without further
/// confirmation by the caller
#[must_use]
pub fn ask_save_file(title: &str, filters: Option<&[(&str, &[&str])]>) -> Option<PathBuf> {
    ask_single_op(OpKind::SaveFile, title, filters)
}

/// Ask for a file to open. Returns None if the user confirmed he wanted to cancel the selection.
///
/// After the user asking for the operation to be cancelled the program may be terminated without further
/// confirmation by the caller
#[must_use]
pub fn ask_open_file(title: &str, filters: Option<&[(&str, &[&str])]>) -> Option<PathBuf> {
    ask_single_op(OpKind::OpenFile, title, filters)
}

/// Ask for a folder to open. Returns None if the user confirmed he wanted to cancel the selection.
///
/// After the user asking for the operation to be cancelled the program may be terminated without further
/// confirmation by the caller
#[must_use]
pub fn ask_open_folder(title: &str, filters: Option<&[(&str, &[&str])]>) -> Option<PathBuf> {
    ask_single_op(OpKind::OpenFolder, title, filters)
}

/// Ask for multiple files to open. Returns None if the user confirmed he wanted to cancel the selection.
///
/// After the user asking for the operation to be cancelled the program may be terminated without further
/// confirmation by the caller
#[must_use]
pub fn ask_open_files(title: &str, filters: Option<&[(&str, &[&str])]>) -> Option<Vec<PathBuf>> {
    ask_multiple_op(MultiOpKind::OpenFiles, title, filters)
}

/// Ask for multiple folders to open. Returns None if the user confirmed he wanted to cancel the selection.
///
/// After the user asking for the operation to be cancelled the program may be terminated without further
/// confirmation by the caller
#[must_use]
pub fn ask_open_folders(title: &str, filters: Option<&[(&str, &[&str])]>) -> Option<Vec<PathBuf>> {
    ask_multiple_op(MultiOpKind::OpenFolders, title, filters)
}

/// Append a collection of filters with (name, extensions) to a `FileDialog`
fn append_filters(filters: Option<&[(&str, &[&str])]>, mut dialog: FileDialog) -> FileDialog {
    if let Some(filters) = filters {
        for (name, ext) in filters.iter().copied() {
            dialog = dialog.add_filter(name, ext);
        }
    }

    dialog
}

fn confirm_cancel() -> MessageDialogResult {
    MessageDialog::new()
        .set_title("Cancelar y salir")
        .set_description("¿Seguro que quieres cancelar y terminar el programa?")
        .set_buttons(rfd::MessageButtons::YesNo)
        .show()
}
