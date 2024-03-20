use bstr::ByteSlice;
use calamine::{open_workbook_auto, DataType, Reader};
use clap::{Parser, Subcommand};
use rust_xlsxwriter::{Workbook, Worksheet};
use std::io::Write;
use std::{collections::BTreeSet, ops::Not, path::PathBuf};

#[derive(Parser)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List available sheets in path
    Sheets {
        /// Path of the xlsx-like file
        path: PathBuf,
    },
    /// List available headers in a sheet from the specified path
    Headers {
        /// The path of the xlsx-like file
        path: PathBuf,
        /// The name of the sheet
        sheet: String,
    },
    /// Print unique values for the given columns from a sheet in a path
    Uniques {
        /// The path of the xlsx-like file
        path: PathBuf,
        /// The name of the sheet
        sheet: String,
        /// The headers to scan for unique values
        headers: Vec<String>,
        /// Path to save the result. The file extension matters
        #[clap(short, long)]
        save: Option<PathBuf>,
    },
    #[clap(hide = true)]
    CompleteSheets { path: PathBuf },
    #[clap(hide = true)]
    CompleteHeaders { path: PathBuf, sheet: String },
}

use std::path::Path;

fn expand_tilde<P: AsRef<Path>>(path_user_input: P) -> Option<PathBuf> {
    let p = path_user_input.as_ref();
    if !p.starts_with("~") {
        return Some(p.to_path_buf());
    }
    if p == Path::new("~") {
        return dirs::home_dir();
    }
    dirs::home_dir().map(|mut h| {
        if h == Path::new("/") {
            // Corner case: `h` root directory;
            // don't prepend extra `/`, just drop the tilde.
            p.strip_prefix("~").unwrap().to_path_buf()
        } else {
            h.push(p.strip_prefix("~/").unwrap());
            h
        }
    })
}

fn main() {
    let Args { command } = Args::parse();

    match command {
        Command::CompleteSheets { ref path } => {
            let path = expand_tilde(path).unwrap();
            let path = &path;
            let sheets = open_workbook_auto(path);

            if sheets.is_err() {
                println!("{:?} para la ruta {:?}", sheets.err(), path.display());
                std::process::exit(0);
            }

            let sheets = sheets.unwrap();

            let sheets = sheets.sheet_names();
            for sheet in sheets {
                println!("{sheet}");
            }
        }
        Command::CompleteHeaders {
            ref path,
            ref sheet,
        } => {
            let path = expand_tilde(path).unwrap();
            let path = &path;
            let sheets = open_workbook_auto(path);

            if sheets.is_err() {
                println!("{:?} para la ruta {:?}", sheets.err(), path.display());
                std::process::exit(0);
            }

            let mut sheets = sheets.unwrap();
            let sheet = sheets.worksheet_range(sheet);
            if sheet.is_err() {
                println!("{:?} para la ruta {:?}", sheet.err(), path.display());
                std::process::exit(0);
            }

            let sheet = sheet.unwrap();
            let mut rows = sheet
                .rows()
                .filter(|row| row.first().is_some_and(|c| c.is_empty().not()));

            let Some(headers) = rows.next() else {
                eprintln!("No hay encabezado en la hoja {sheet:?}");
                return;
            };

            let headers = headers
                .iter()
                .map(|v| match v {
                    calamine::Data::String(v) => v.to_owned(),
                    v => v.to_string(),
                })
                .collect::<Vec<_>>();

            for header in headers {
                println!("{header}");
            }
        }
        Command::Sheets { ref path } => {
            let sheets = open_workbook_auto(path).unwrap();
            let sheets = sheets.sheet_names();
            println!("{:#?}", sheets)
        }
        Command::Headers { path, sheet } => {
            let mut sheets = open_workbook_auto(path).unwrap();
            let Ok(sheet) = sheets.worksheet_range(&sheet) else {
                eprintln!("Hubo un problema abriendo {sheet:?}, seguro que existe?");
                return;
            };

            let mut rows = sheet
                .rows()
                .filter(|row| row.first().is_some_and(|c| c.is_empty().not()));

            let Some(headers) = rows.next() else {
                eprintln!("No hay encabezado en la hoja {sheet:?}");
                return;
            };

            let headers = headers
                .iter()
                .map(|v| match v {
                    calamine::Data::String(v) => v.to_owned(),
                    v => v.to_string(),
                })
                .collect::<Vec<_>>();

            println!("{:#?}", headers);
        }
        Command::Uniques {
            path,
            sheet,
            headers,
            save,
        } => {
            let mut sheets = open_workbook_auto(path).unwrap();
            let Ok(sheet) = sheets.worksheet_range(&sheet) else {
                eprintln!("Hubo un problema abriendo {sheet:?}, seguro que existe?");
                return;
            };

            let mut rows = sheet.rows().filter(|row| row.iter().any(|a| !a.is_empty()));
            // .filter(|row| row.first().is_some_and(|c| c.is_empty().not()));

            let Some(available_headers) = rows.next() else {
                eprintln!("No hay encabezado en la hoja {sheet:?}");
                return;
            };

            let available_headers = available_headers
                .iter()
                .map(|v| match v {
                    calamine::Data::String(v) => v.to_owned(),
                    v => v.to_string(),
                })
                .collect::<Vec<_>>();

            let mut finals = Vec::new();

            for (n_col, header) in headers.iter().enumerate() {
                let Some(col) = available_headers.iter().position(|h| h == header) else {
                    eprintln!("Error: Can not find header {header:?}, skipping.");
                    continue;
                };

                rows.clone()
                    .map(|row| &row[col])
                    .map(|v| match v {
                        calamine::Data::String(v) => v.to_string(),
                        v => v.to_string(),
                    })
                    .enumerate()
                    .for_each(|(row, v)| {
                        let row = if let Some(v) = finals.get_mut(row) {
                            v
                        } else {
                            let len = finals.len();
                            finals.reserve(row - finals.len());
                            for _ in len..row + 1 {
                                finals.push(vec![String::new(); headers.len()]);
                            }
                            finals.get_mut(row).unwrap()
                        };

                        row[n_col] = v;
                    })
            }

            let mut vfinal = Vec::new();
            let n_head = headers.len();
            vfinal.push(headers);
            vfinal.extend(finals.into_iter().collect::<BTreeSet<Vec<_>>>());

            let Some(save_path) = save else {
                for r in vfinal {
                    for (i, c) in r.iter().enumerate() {
                        print!("{c:?}{}", if i + 1 < n_head { "," } else { "" });
                    }
                    print!("\n");
                }
                return;
            };

            match save_path
                .as_os_str()
                .as_encoded_bytes()
                .rsplit_once_str(".")
            {
                Some((_, ext)) => match ext {
                    b"csv" => {
                        let mut file = std::fs::File::create(save_path).unwrap();
                        for vec in vfinal {
                            for (i, v) in vec.iter().enumerate() {
                                if i < vec.len() - 1 {
                                    write!(file, "{v:#?},").unwrap();
                                } else {
                                    write!(file, "{v:#?}").unwrap();
                                }
                            }
                            writeln!(file).unwrap();
                        }
                    }
                    b"xlsx" => {
                        let mut workook = Workbook::new();
                        let mut worksheet = Worksheet::new();
                        worksheet.set_name("Uniques").unwrap();

                        for (row, values) in vfinal.iter().enumerate() {
                            for (col, value) in values.iter().enumerate() {
                                worksheet
                                    .write(row.try_into().unwrap(), col.try_into().unwrap(), value)
                                    .unwrap();
                            }
                        }

                        workook.push_worksheet(worksheet);
                        workook.save(save_path).unwrap();
                    }
                    _ => {
                        eprintln!("Invalid extension {ext:#?}");
                    }
                },
                None => {
                    eprintln!("Error: El archivo no tiene extensión")
                }
            }
        }
    }
}
