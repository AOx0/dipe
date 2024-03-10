// #![deny(clippy::all)]
// #![warn(clippy::pedantic)]
// #![deny(rust_2018_idioms, unsafe_code)]
// #![deny(clippy::unwrap_used)]
// #![allow(clippy::too_many_lines)]
#![windows_subsystem = "windows"]

use bstr::{BStr, BString, ByteSlice};
use calamine::{open_workbook_auto, DataType, Reader};
use calamine::{Data, Sheets};
use clap::Parser;
use itertools::Itertools;
use rfd::{FileDialog, MessageButtons, MessageDialog, MessageDialogResult, MessageLevel};
use rust_xlsxwriter::{Color, ExcelDateTime, Workbook, Worksheet};
use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::AtomicBool;
use std::{
    collections::HashMap,
    env::current_dir,
    ffi::OsStr,
    ops::Not,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::atomic::{AtomicUsize, Ordering},
};
use strings::get_words;
use thiserror::Error;
use walkdir::WalkDir;

static NUMERO_CAMPUS: AtomicUsize = AtomicUsize::new(0);
static FROM_CLI: AtomicBool = AtomicBool::new(false);

#[derive(Parser)]
struct Args {
    rutas: Vec<PathBuf>,
    // Directorio donde poner los concentrados
    #[clap(long, short)]
    salida: Option<PathBuf>,
    // Número de campus de la escuela
    #[clap(long, short, default_value = "3")]
    numero_campus: usize,
    // Escribir descartados
    #[clap(long, short)]
    descartados: bool,
    // No mostrar dialogos nativos
    #[clap(long, short)]
    cli: bool,
    // Reset destination directory
    #[clap(long, short)]
    reset: bool,
}

#[derive(Error, Debug)]
enum MiError {
    #[error("Error de IO: ")]
    IoError(#[from] std::io::Error),
}

#[derive(Debug)]
struct PreGrupo {
    rutas: Vec<PathBuf>,
    headers: Vec<Vec<String>>,
}

fn main() -> ExitCode {
    let Args {
        mut rutas,
        numero_campus,
        salida,
        descartados: mut descatrados,
        cli,
        mut reset,
    } = Args::parse();

    FROM_CLI.store(
        cli || (rutas.is_empty().not() || descatrados || salida.is_some()),
        Ordering::Relaxed,
    );
    NUMERO_CAMPUS.store(numero_campus, Ordering::Relaxed);

    let salida = match get_directorio_salida(salida) {
        Ok(value) => value,
        Err(value) => return value,
    };

    if salida.read_dir().is_ok_and(|dir| dir.count() != 0) {
        if !FROM_CLI.load(Ordering::Relaxed) {
            let res = MessageDialog::new()
                .set_title("¿Reemplazar directorio?")
                .set_description("¿El directorio no está vacío, deseas eliminar su contenido? Si eliges que no, solo se sobreescribirán los archivos existentes.")
                .set_level(MessageLevel::Warning)
                .set_buttons(MessageButtons::YesNo)
                .show();

            reset = matches!(res, MessageDialogResult::Yes);
        }

        if reset {
            std::fs::remove_dir_all(&salida).unwrap();
            std::fs::create_dir_all(&salida).unwrap();
        }
    }

    if rutas.is_empty() && !FROM_CLI.load(Ordering::Relaxed) {
        if let Some(exit_code) = dialogo_pedir_rutas_entrada(&mut rutas) {
            return exit_code;
        }
    }

    if !FROM_CLI.load(Ordering::Relaxed) {
        let res = MessageDialog::new()
            .set_title("Descartados")
            .set_description("¿Deseas escribir las filas descartadas?")
            .set_level(MessageLevel::Info)
            .set_buttons(MessageButtons::YesNo)
            .show();

        descatrados = matches!(res, MessageDialogResult::Yes);
    };

    let grupos = obtener_grupos_por_nombre(&rutas, &["xlsx", "xlsm", "xlsb", "xls"]);
    let mut grupos = grupos
        .into_iter()
        .map(|(k, v)| {
            (k, {
                let headers = headers_from_file(&v[0]);
                PreGrupo { rutas: v, headers }
            })
        })
        .collect::<HashMap<_, _>>();

    // Agrupamos los grupos que tienen los mismos encabezados
    'a: loop {
        let keys = grupos.keys().cloned().enumerate().collect_vec();
        for (i, key1) in &keys {
            for (j, key2) in &keys {
                if i == j {
                    continue;
                }

                let a = &grupos.get(key1).as_ref().unwrap().headers;
                let b = &grupos.get(key2).as_ref().unwrap().headers;

                let similarity = a
                    .iter()
                    .zip(b.iter())
                    .map(|(a, b)| 1. - (vec_differences(a, b) as f64 / a.len().max(b.len()) as f64))
                    .sum::<f64>();

                let avg_similarity = similarity / a.len().max(b.len()) as f64;
                if (avg_similarity >= 0.8 && a.len() - b.len() == 0) || a == b {
                    let final_key = if key1.len() >= key2.len() { key1 } else { key2 }.to_owned();

                    let mut val1 = grupos.remove(key1).unwrap();
                    let val2 = grupos.remove(key2).unwrap();

                    val1.rutas.extend(val2.rutas.into_iter());
                    grupos.insert(final_key, val1);

                    continue 'a;
                }
            }
        }

        break;
    }

    let mut grupos = grupos
        .into_iter()
        .map(|(k, v)| (k, v.rutas))
        .collect::<HashMap<_, _>>();

    // Logeamos los grupos
    log_grupos(&salida, &grupos);
    // Descartamos los grupos que no tienen más de un archivo
    grupos.retain(|_, v| v.len() > 1);

    std::thread::scope(|s| {
        for (grupo, rutas) in &mut grupos {
            s.spawn(|| {
                let mut rutas = rutas
                    .iter_mut()
                    .map(|ruta| {
                        let ruta = std::mem::take(ruta);
                        let excel = open_workbook_auto(&ruta).unwrap();
                        (ruta, excel)
                    })
                    .collect_vec();

                let headers = extract_headers(&mut rutas);
                let mut name_buff = Vec::new();

                name_buff.clear();
                name_buff.extend_from_slice(grupo.as_bstr());
                name_buff.extend_from_slice(b".xlsx".as_slice());

                let xlsx_salida = concat_path(&salida, unsafe {
                    OsStr::from_encoded_bytes_unchecked(&name_buff)
                });

                name_buff.clear();
                name_buff.extend_from_slice(grupo.as_bstr());
                name_buff.extend_from_slice(b".descartados.xlsx".as_slice());

                let xlsx_descartados = concat_path(&salida, unsafe {
                    OsStr::from_encoded_bytes_unchecked(&name_buff)
                });

                write_to_file(
                    &mut rutas,
                    &headers,
                    &xlsx_salida,
                    descatrados.then_some(&xlsx_descartados),
                );
            });
        }
    });

    if FROM_CLI.load(Ordering::Relaxed) {
        println!("Listo!!");
    } else {
        let _ = MessageDialog::new()
            .set_title("Listo")
            .set_description("Listo!!")
            .set_level(MessageLevel::Info)
            .show();
    }

    ExitCode::SUCCESS
}

fn dialogo_pedir_rutas_entrada(rutas: &mut Vec<PathBuf>) -> Option<ExitCode> {
    loop {
        let multiples = MessageDialog::new()
            .set_title("Selección manual")
            .set_description("¿Deseas selccionar los archivos a concentrar manualmente? Si eliges que no solo seleccionarás el directorio que contiene todos los archivos")
            .set_level(MessageLevel::Info)
            .set_buttons(MessageButtons::YesNo)
            .show();

        let multiples = matches!(multiples, MessageDialogResult::Yes);

        let vec = if multiples {
            FileDialog::new()
                .set_title("Selecciona los archivos")
                .add_filter("Excel", &["xlsx"])
                .pick_files()
        } else {
            FileDialog::new()
                .set_title("Selecciona la carpeta con los archivos")
                .pick_folder()
                .map(|a| vec![a])
        };
        let Some(res) = vec else {
            let cancelar = MessageDialog::new()
                .set_title("Cancelar")
                .set_description("¿Deseas cancelar la acción y salir del programa?")
                .set_level(MessageLevel::Warning)
                .set_buttons(MessageButtons::YesNo)
                .show();

            let cancelar = matches!(cancelar, MessageDialogResult::Yes);

            if cancelar {
                return Some(ExitCode::SUCCESS);
            }
            continue;
        };

        let _ = std::mem::replace(rutas, res);
        break;
    }
    None
}

#[derive(Debug)]
struct ColumnInfo {
    index: u16,
    siempre_lleno: bool,
    valor_unico: bool,
    numero_datos: u32,
}

#[derive(Debug)]
struct Headers {
    encabezados: HashMap<String, HashMap<PathBuf, HashMap<String, ColumnInfo>>>,
    encabezados_unicos: HashMap<String, Vec<String>>,
    hojas: Vec<String>,
}

fn vec_differences<'a, T: PartialEq>(mut a: &'a [T], mut b: &'a [T]) -> usize {
    if a.len() > b.len() {
        std::mem::swap(&mut a, &mut b);
    }

    b.iter().filter(|b| a.contains(b).not()).count()
}

fn write_to_file(
    rutas: &mut [(PathBuf, Sheets<BufReader<File>>)],
    headers: &Headers,
    salida: &Path,
    descartados: Option<&Path>,
) {
    let mut workbook = Workbook::new();
    let mut wb_descartados = Workbook::new();
    let mut sheet_descartados = Worksheet::new();
    sheet_descartados
        .set_name("Descartados")
        .unwrap()
        .autofit()
        .set_freeze_panes(1, 0)
        .unwrap()
        .set_row_height(0, 35)
        .unwrap();
    wb_descartados.push_worksheet(sheet_descartados);

    let sheet_descartados = wb_descartados.worksheet_from_name("Descartados").unwrap();
    let mut c_row_descartados = 0;

    for hoja in &headers.hojas {
        if get_words(&hoja.to_lowercase())
            .next()
            .is_some_and(|a| a.starts_with("desp"))
        {
            if FROM_CLI.load(Ordering::Relaxed) {
                println!("Descartando hoja {hoja:?} porque es de desplegables");
            }
            continue;
        }

        if workbook.worksheet_from_name(hoja).is_err() {
            let mut worksheet = Worksheet::new();
            worksheet.set_name(hoja).unwrap();
            workbook.push_worksheet(worksheet);
        }

        let worksheet = workbook.worksheet_from_name(hoja).unwrap();

        let encabezados = headers.encabezados.get(hoja.trim()).unwrap();
        let encabezados_unicos = headers.encabezados_unicos.get(hoja.trim()).unwrap();

        let format = rust_xlsxwriter::Format::new()
            .set_bold()
            .set_background_color(Color::RGB(0x00EE_ECE1))
            .set_align(rust_xlsxwriter::FormatAlign::Left);

        for (col, encabezado_unico) in encabezados_unicos.iter().enumerate() {
            worksheet
                .write_with_format(0, col.try_into().unwrap(), encabezado_unico, &format)
                .unwrap();
        }

        let mut c_row = 1;

        for (ruta, excel) in rutas.iter_mut() {
            let Some(columnas) = encabezados.get(ruta) else {
                continue;
            };
            let Ok(range) = excel.worksheet_range(hoja) else {
                continue;
            };

            let mut rows = range
                .rows()
                .filter(|row| row.first().is_some_and(|cell| cell.is_empty().not()));

            let headers_row = rows
                .next()
                .unwrap()
                .iter()
                .enumerate()
                .filter_map(|(i, a)| a.as_string().map(|a| (i, a)))
                .collect_vec();
            let iter_rows = rows.clone().filter(|row| {
                let all_empty = row.iter().zip(headers_row.iter()).all(|(data, a)| {
                    let col_info = columnas.get(&a.1).unwrap();
                    vacio(data, col_info)
                });

                !all_empty
            });

            // println!("Ruta: {:?}", ruta.file_name());

            for invalid_row in rows.filter(|row| {
                let all_empty = row.iter().zip(headers_row.iter()).all(|(data, a)| {
                    let col_info = columnas.get(&a.1).unwrap();
                    vacio(data, col_info)
                });

                all_empty
            }) {
                c_row_descartados += 1;
                if descartados.is_none() {
                    // println!("Descartando fila {c_row_descartados} en {}: {invalid_row:?}", ruta.display());
                } else {
                    for (col, cell) in invalid_row.iter().enumerate() {
                        let format = rust_xlsxwriter::Format::new();
                        write_cell(
                            cell,
                            sheet_descartados,
                            c_row_descartados,
                            col.try_into().unwrap(),
                            &format,
                        );
                    }
                }
            }

            let mut added = 0;
            for (final_col, encabezado) in encabezados_unicos.iter().enumerate() {
                let Some(ColumnInfo { index, .. }) = columnas.iter().find_map(|(name, info)| {
                    get_words(&name.to_lowercase())
                        .eq(get_words(&encabezado.to_lowercase()))
                        .then_some(info)
                }) else {
                    // println!("MISS: La columna {encabezado} no existe en \"{}\":{}", ruta.display(), hoja);
                    continue;
                };

                for (i, row) in iter_rows.clone().enumerate() {
                    let cell = &row[*index as usize];
                    let n_row = c_row + i;
                    let n_col = final_col.try_into().expect("Esto no debería pasar nunca");
                    let format = rust_xlsxwriter::Format::new();

                    write_cell(cell, worksheet, n_row.try_into().unwrap(), n_col, &format);

                    if n_row > added {
                        added = i;
                    }
                }
            }

            c_row += added + 1;
            // println!("Continuando: {}", c_row);
        }

        worksheet
            .autofit()
            // El primer renglón es el encabezado, asi que lo fijamos
            .set_freeze_panes(1, 0)
            .unwrap()
            // El primer renglón es el encabezado, así que lo destacamos
            .set_row_height(0, 35)
            .unwrap()
            .set_tab_color({
                match hoja.trim() {
                    "DATOS" => Color::RGB(0x0033_3333),
                    "GLOSARIO" => Color::RGB(0x0096_0C22),
                    _ => Color::RGB(0x0000_3087),
                }
            });
    }

    if FROM_CLI.load(Ordering::Relaxed) {
        println!("Escribiendo {}", salida.display());
    }

    workbook.save(salida).unwrap();
    if let Some(descartados) = descartados {
        if FROM_CLI.load(Ordering::Relaxed) {
            println!("Escribiendo {}", descartados.display());
        }
        wb_descartados.save(descartados).unwrap();
    }
}

fn headers_from_file(ruta: &Path) -> Vec<Vec<String>> {
    let mut excel = open_workbook_auto(ruta).unwrap();
    let sheets = excel.sheet_names();
    let mut res = Vec::with_capacity(sheets.len());

    for sheet_name in sheets.iter() {
        let lowercase = &sheet_name.trim().to_lowercase();
        if lowercase.starts_with("desp")
            || lowercase.contains("glosario")
            || lowercase.contains("siglas")
            || lowercase.contains("diccionario")
            || (lowercase.contains("hoja") && sheets.len() > 1)
            || (lowercase.contains("logo") && lowercase.contains("cat"))
            || lowercase.contains("datos")
        {
            continue;
        }

        let sheet = excel
            .worksheet_range(sheet_name)
            .expect("We just read the sheet name");

        let mut filter = sheet
            .rows()
            .filter(|row| row.first().is_some_and(|cell| cell.is_empty().not()))
            .filter(|row| {
                row.first().is_some_and(|cell| {
                    cell.as_string()
                        .is_some_and(|val| val.trim().is_empty().not())
                })
            });

        let Some(header) = filter.next() else {
            continue;
        };

        let headers = header
            .iter()
            .filter_map(|val| {
                if let Data::String(val) = val {
                    Some(
                        get_words(val.trim())
                            .map(|a| {
                                a.chars()
                                    .filter(|a| a.is_alphabetic())
                                    .map(|a| a.to_lowercase().next().unwrap_or_default())
                                    .next()
                                    .unwrap_or_default()
                            })
                            .take(3)
                            .join(""),
                    )
                } else {
                    None
                }
            })
            .collect_vec();

        res.push(headers);
    }

    res
}

fn vacio(data: &Data, col_info: &ColumnInfo) -> bool {
    let es_cero = |a: &Data| {
        a.is_int() && a.as_i64().is_some_and(|a| a == 0)
            || a.is_float() && a.as_f64().is_some_and(|a| a == 0.)
            || a.as_string()
                .is_some_and(|a| a.trim() == "0" || a.contains("#DIV/0!"))
    };

    data.is_empty()
        || es_cero(data)
        || (data.as_string().is_some_and(|a| a.trim().is_empty()))
        || (col_info.valor_unico && col_info.siempre_lleno && col_info.numero_datos != 1)
}

fn write_cell<'a>(
    cell: &'a Data,
    worksheet: &'a mut Worksheet,
    n_row: u32,
    n_col: u16,
    format: &'a rust_xlsxwriter::Format,
) -> Option<Result<&'a mut Worksheet, rust_xlsxwriter::XlsxError>> {
    Some(match cell {
        Data::Int(val) => worksheet.write_with_format(n_row, n_col, *val, format),
        Data::Float(val) => worksheet.write_with_format(n_row, n_col, *val, format),
        Data::String(val) | Data::DateTimeIso(val) | Data::DurationIso(val) => {
            worksheet.write_with_format(n_row, n_col, val, format)
        }
        Data::Bool(val) => worksheet.write_with_format(n_row, n_col, *val, format),
        Data::DateTime(val) => {
            let format = format.clone().set_num_format("dd/mm/yyyy");

            worksheet.write_datetime_with_format(
                n_row,
                n_col,
                if let Ok(d) = ExcelDateTime::from_serial_datetime(val.as_f64()) {
                    d
                } else {
                    println!("Fecha invalida en fila {val}");
                    ExcelDateTime::from_ymd(1900, 1, 1).unwrap()
                },
                &format,
            )
        }
        Data::Error(val) => {
            println!("Error: {val:?}");
            worksheet.write_with_format(n_row, n_col, "", format)
        }
        Data::Empty => return None,
    })
}

fn extract_headers(rutas: &mut Vec<(PathBuf, Sheets<BufReader<File>>)>) -> Headers {
    let mut encabezados = Headers {
        encabezados: HashMap::new(),
        encabezados_unicos: HashMap::new(),
        hojas: Vec::new(),
    };

    for (ruta, excel) in rutas {
        // println!("Encabezados {}", ruta.display());

        for sheet in excel.sheet_names() {
            // Si la hoja no está en el vector de hojas, lo agregamos
            if !encabezados
                .hojas
                .iter()
                .any(|hoja| get_words(&hoja.to_lowercase()).eq(get_words(&sheet.to_lowercase())))
            {
                encabezados.hojas.push(sheet.clone());
            }

            let entrada = encabezados.encabezados.entry(sheet.clone()).or_default();
            let entrada_unicos = encabezados
                .encabezados_unicos
                .entry(sheet.clone())
                .or_default();

            // println!("Encabezados {}:{}", ruta.display(), sheet);
            let range = excel.worksheet_range(&sheet).unwrap();

            let mut rows = range
                .rows()
                .filter(|row| row.first().is_some_and(|cell| cell.is_empty().not()));
            let Some(encabezados) = rows.next() else {
                println!("Error: Hoja sin encabezado en {}:{sheet}", ruta.display());
                continue;
            };

            for (col, encabezado) in encabezados.iter().enumerate() {
                let Some(encabezado) = encabezado.as_string() else {
                    if rows
                        .clone()
                        .map(|row| &row[col])
                        .any(|cell| cell.is_empty().not())
                    {
                        // println!("Encabezado con columna no vacía en {}:{}:{}", ruta.display(), sheet, col);
                    }
                    continue;
                };

                if entrada_unicos.iter().any(|unico| {
                    get_words(&unico.to_lowercase()).eq(get_words(&encabezado.to_lowercase()))
                }) {
                    // println!("Encabezado repetido en {}:{}:{}", ruta.display(), sheet, encabezado);
                } else if encabezado.trim().is_empty().not() {
                    entrada_unicos.push(encabezado.clone());
                }

                let encabezados = entrada.entry(ruta.clone()).or_default();

                let Ok(col): Result<u16, _> = col.try_into() else {
                    println!(
                        "Error: Demasiadas columnas en {}:{}:{}",
                        ruta.display(),
                        sheet,
                        col
                    );
                    continue;
                };

                let valor_unico = rows
                    .clone()
                    .map(|row| &row[col as usize])
                    .map(|cell| {
                        cell.as_string()
                            .map(|a| get_words(a.trim()).join(" ").to_lowercase())
                            .unwrap_or_default()
                    })
                    .unique()
                    .count()
                    == 1;
                let siempre_lleno = rows.clone().all(|row| {
                    row[col as usize].is_empty().not()
                        && row[col as usize]
                            .as_string()
                            .is_some_and(|s| s.is_empty().not())
                });
                let numero_datos = rows
                    .clone()
                    .map(|row| &row[col as usize])
                    .filter(|cell| cell.is_empty().not())
                    .count()
                    .try_into()
                    .expect("No debería pasar nunca");

                encabezados.insert(
                    encabezado.to_string(),
                    ColumnInfo {
                        index: col,
                        siempre_lleno,
                        valor_unico,
                        numero_datos,
                    },
                );
            }
        }
    }

    // println!("{encabezados:#?}");

    encabezados
}

fn log_grupos(salida: &Path, grupos: &HashMap<BString, Vec<PathBuf>>) {
    let mut grupos_excel = Workbook::new();

    let mut worksheet = Worksheet::new();
    let mut current_row = 0u32;

    let format = rust_xlsxwriter::Format::new()
        .set_bold()
        .set_background_color(Color::RGB(0x00EE_ECE1))
        .set_align(rust_xlsxwriter::FormatAlign::Left);

    for (i, &encabezado) in ["Grupo", "Estatus", "Nombre", "Ruta"].iter().enumerate() {
        worksheet
            .write_string_with_format(current_row, i.try_into().unwrap(), encabezado, &format)
            .unwrap();
    }

    for (grupo, rutas) in grupos {
        let grupo = String::from_utf8_lossy(grupo.as_bstr().as_bytes()).to_string();
        let incluido = rutas.len() > 1;

        for ruta in rutas.iter() {
            // We write name, path
            current_row += 1;
            worksheet.write_string(current_row, 0, &grupo).unwrap();

            worksheet
                .write_string(
                    current_row,
                    1,
                    if incluido { "Incluido" } else { "Descartado" },
                )
                .unwrap();

            worksheet
                .write_string(current_row, 2, ruta.file_name().unwrap().to_string_lossy())
                .unwrap();
            worksheet
                .write_string(current_row, 3, ruta.display().to_string())
                .unwrap();
        }
    }

    worksheet
        .autofit()
        // El primer renglón es el encabezado, asi que lo fijamos
        .set_freeze_panes(1, 0)
        .unwrap()
        // El primer renglón es el encabezado, así que lo destacamos
        .set_row_height(0, 35)
        .unwrap();

    grupos_excel.push_worksheet(worksheet);

    grupos_excel
        .save(concat_path(salida, "grupos.xlsx".as_ref()))
        .unwrap();
}

fn get_directorio_salida(salida: Option<PathBuf>) -> Result<PathBuf, ExitCode> {
    let from_cli = FROM_CLI.load(Ordering::Relaxed);

    loop {
        let salida = if let Some(salida) = salida {
            salida
        } else if !from_cli {
            let salida = FileDialog::new()
                .set_title("¿Dónde almacenar los generados?")
                .set_directory(&current_dir().unwrap())
                .pick_folder();

            let Some(salida) = salida else {
                let continuar = MessageDialog::new()
                    .set_title("¿Volver a intentar?")
                    .set_description("No elegiste ningún archivo. ¿Quieres volver a intentar?")
                    .set_buttons(MessageButtons::YesNo)
                    .show();

                let continuar = matches!(continuar, MessageDialogResult::Yes);

                if continuar {
                    continue;
                }

                break Err(ExitCode::SUCCESS);
            };

            salida
        } else {
            current_dir().unwrap()
        };

        if from_cli {
            if !salida.exists() {
                std::fs::create_dir_all(&salida).unwrap();
            }

            if !salida.is_dir() {
                println!("Error: La ruta de salida no es un directorio");
                return Err(ExitCode::FAILURE);
            }
        }

        break Ok(salida);
    }
}

fn concat_path(salida: &Path, nombre: &OsStr) -> PathBuf {
    let mut salida = salida.to_path_buf();
    salida.push(nombre);

    salida
}

fn obtener_grupos_por_nombre(
    rutas: &[PathBuf],
    include: &[&str],
) -> HashMap<BString, Vec<PathBuf>> {
    let mut groups: HashMap<BString, Vec<PathBuf>> = HashMap::new();

    for ruta in rutas {
        if ruta.is_file() {
            process_file(ruta.to_owned(), &mut groups);
            continue;
        }
        for file in iter_dir_files(ruta, include) {
            process_file(file, &mut groups);
        }
    }

    groups
}

fn iter_dir_files<'a>(
    ruta: &'a PathBuf,
    include: &'a [&'a str],
) -> impl Iterator<Item = PathBuf> + 'a {
    WalkDir::new(ruta)
        .into_iter()
        .filter_map(|a| {
            let Ok(a) = a else {
                if FROM_CLI.load(Ordering::Relaxed) {
                    eprintln!("Error: {}", a.err().unwrap());
                    #[cfg(target_os = "windows")]
                    eprintln!("Si la ruta la especificaste como `.\\directorio\\` intenta especificarlo como `.\\directorio`, eliminando la última diagonal inversa \\");
                } else {
                    let msg = format!("Hubo un error con la ruta especificada {}", ruta.display());

                    MessageDialog::new()
                        .set_title("Error")
                        .set_level(MessageLevel::Error)
                        .set_description(msg)
                        .show()
                    ;
                }
                return None;
            };
            Some(a)
        })
        .filter(|a| {
            include.iter().any(|extension| {
                BStr::new(a.file_name().as_encoded_bytes()).ends_with(extension.as_bytes())
            })
        })
        .filter_map(|a| a.file_type().is_file().then(|| a.into_path()))
}

fn process_file(entrada: PathBuf, groups: &mut HashMap<BString, Vec<PathBuf>>) {
    let name = get_name(&entrada);
    if !groups.contains_key(name) {
        groups.insert(
            name.to_owned(),
            Vec::with_capacity(NUMERO_CAMPUS.load(Ordering::Relaxed)),
        );
    }

    groups.get_mut(name).unwrap().push(entrada);
}

fn get_name(entrada: &Path) -> &BStr {
    const LEN_EXTENSION: usize = 4;
    const LEN_CICLO: usize = 5;

    let name = BStr::new(entrada.file_name().unwrap().as_encoded_bytes());

    let name = if name.len() > LEN_EXTENSION + LEN_CICLO {
        let (name, _extension) = name.rsplit_once_str(b".").unwrap();
        if name.len() > LEN_CICLO {
            let ciclo = &name[name.len() - 5..];

            if ciclo[2] == b'-' {
                let name = &name[..name.len() - 5];
                let name = name.trim_end_with(|c| c == '_');
                let name = &name[..name.len() - 3];
                let name = name.trim_end_with(|c| c == '_');
                name
            } else {
                name
            }
        } else {
            name
        }
    } else {
        name
    };

    let name = BStr::new(name);
    name
}

#[cfg(test)]
mod tests {
    #[test]
    fn vec_diff() {
        let a = vec![1, 2, 3, 4, 5];
        let b = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];

        assert_eq!(super::vec_differences(&a, &b), 4);

        let b = vec![1, 2, 3, 4, 5, 6, 7];
        let a = vec![1, 2, 4, 5];

        assert_eq!(super::vec_differences(&a, &b), 3);
    }
}
