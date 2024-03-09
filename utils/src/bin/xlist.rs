use std::{collections::BTreeSet, ops::Not, path::PathBuf};

use calamine::{open_workbook_auto, DataType, Reader};
use clap::{Parser, Subcommand};

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
    },
}

fn main() {
    let Args { command } = Args::parse();

    match command {
        Command::Sheets { path } => {
            let sheets = open_workbook_auto(path).unwrap();
            println!("{:#?}", sheets.sheet_names())
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
        } => {
            let mut sheets = open_workbook_auto(path).unwrap();
            let Ok(sheet) = sheets.worksheet_range(&sheet) else {
                eprintln!("Hubo un problema abriendo {sheet:?}, seguro que existe?");
                return;
            };

            let mut rows = sheet
                .rows()
                .filter(|row| row.first().is_some_and(|c| c.is_empty().not()));

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
            vfinal.push(headers);
            vfinal.extend(finals.into_iter().collect::<BTreeSet<Vec<_>>>());

            println!("{vfinal:#?}")
        }
    }
}
