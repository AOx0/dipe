#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![deny(rust_2018_idioms, unsafe_code)]
//#![deny(clippy::unwrap_used)]
#![allow(clippy::too_many_lines)]
#![windows_subsystem = "windows"]

use calamine::{open_workbook, Data, DataRef, DataType, Range, Reader};
use clap::Parser;
use dialogs::{ask_open_file, ask_open_files, ask_save_file};
use itertools::{chain, izip, Itertools};
use rust_xlsxwriter::Workbook;
use std::collections::{HashMap, HashSet};
use std::ops::Not;
use std::path::PathBuf;

const UNI: &str = "NOMBRE INSTITUCIÓN";
const CAMPO_AMPLIO: &str = "CAMPO AMPLIO DE FORMACIÓN";
const CAMPO_ESPECIFICO: &str = "CAMPO ESPECÍFICO DE FORMACIÓN";
const CAMPO_DETALLADO: &str = "CAMPO DETALLADO DE FORMACIÓN";
const NIVEL: &str = "NIVEL DE ESTUDIOS";

#[derive(Debug)]
struct Config {
    universidades: HashMap<String, String>,
    niveles_activos: HashMap<String, bool>,
}

#[derive(Parser)]
struct Args {
    /// La ruta de los archivos a procesar
    rutas: Vec<PathBuf>,

    /// La ruta del archivo de salida
    #[clap(short, long)]
    salida: Option<PathBuf>,

    /// El excel con el mapeo de las universidades y los niveles activos
    #[clap(short, long)]
    config: Option<PathBuf>,
}

type Related<'a> = HashMap<(&'a str, &'a str, &'a str, &'a str), u64>;

fn get_unique_relations<'a>(
    headers: &'a [String],
    paths: &'a [PathBuf],
    strings: &'a HashSet<String>,
) -> Related<'a> {
    let mut res = Related::with_capacity(150);
    let mut current_id = 0;

    for path in paths {
        let mut xl = open_excel(path);
        let range = get_first_sheet(&mut xl);

        let campo_amplio = get_col_named(headers, &range, CAMPO_AMPLIO);
        let campo_detallado = get_col_named(headers, &range, CAMPO_DETALLADO);
        let campo_especifico = get_col_named(headers, &range, CAMPO_ESPECIFICO);
        let nivel = get_col_named(headers, &range, NIVEL);

        let chain = izip!(
            campo_amplio.rows().skip(4).flatten(),
            campo_detallado.rows().skip(4).flatten(),
            campo_especifico.rows().skip(4).flatten(),
            nivel.rows().skip(4).flatten()
        );

        for (amplio, detallado, especifico, nivel) in chain {
            if matches!(amplio, DataRef::Empty) {
                continue;
            }

            let key = (
                strings.get(amplio.get_string().unwrap()).unwrap().as_str(),
                strings
                    .get(detallado.get_string().unwrap())
                    .unwrap()
                    .as_str(),
                strings
                    .get(especifico.get_string().unwrap())
                    .unwrap()
                    .as_str(),
                strings
                    .get(get_nivel_name(nivel.get_string().unwrap()))
                    .unwrap()
                    .as_str(),
            );

            res.entry(key).or_insert({
                current_id += 1;
                current_id - 1
            });
        }
    }

    res
}

fn get_headers(ruta: &std::path::Path) -> Vec<String> {
    let mut xl = open_excel(ruta);
    let range = get_first_sheet(&mut xl);
    let first_row = range.rows().nth(3).unwrap();

    let mut vec = vec!["Ciclo".to_string()];
    vec.extend(first_row.iter().map(|v| match v.get_string() {
        Some(v) => v.to_string(),
        _ => unreachable!("{v:?}"),
    }));

    vec.push("Relacion".to_string());
    vec
}

fn get_first_sheet(
    xl: &mut calamine::Xlsx<std::io::BufReader<std::fs::File>>,
) -> Range<DataRef<'_>> {
    let sheet_names = xl.sheet_names();
    let name = sheet_names.first().unwrap();
    xl.worksheet_range_ref(name).unwrap()
}

fn make_persistent_related<'a>(
    headers: &[String],
    paths: &'a [PathBuf],
    strings: &'a mut HashSet<String>,
) {
    for path in paths {
        let mut xl = open_excel(path);
        let range = get_first_sheet(&mut xl);

        let campo_amplio = get_col_named(headers, &range, CAMPO_AMPLIO);
        let campo_detallado = get_col_named(headers, &range, CAMPO_DETALLADO);
        let campo_especifico = get_col_named(headers, &range, CAMPO_ESPECIFICO);
        let nivel = get_col_named(headers, &range, NIVEL);

        let chain = chain!(
            campo_amplio.rows().skip(4).flatten(),
            campo_detallado.rows().skip(4).flatten(),
            campo_especifico.rows().skip(4).flatten(),
        );

        // println!("{campo_amplio:?}");

        for value in chain {
            if let DataRef::String(value) = value {
                make_persistent(strings, value);
            }
        }

        for nivel in nivel.rows().skip(4).flatten() {
            if let DataRef::String(value) = nivel {
                make_persistent(strings, get_nivel_name(value.as_str()));
            }
        }
    }
}

fn get_col_named<'xl>(
    headers: &[String],
    range: &Range<DataRef<'xl>>,
    name: &str,
) -> Range<DataRef<'xl>> {
    let col = get_relevant_id_u32(headers, name);
    range.range((0, col), (range.height().try_into().unwrap(), col))
}

fn read_config_xlsx(ruta: &std::path::Path) -> Config {
    let mut xl = open_excel(ruta);
    let sheets = xl.sheet_names();
    let sheets_lower = sheets.iter().map(|a| a.to_lowercase()).collect_vec();

    let mut universidades = HashMap::new();
    let mut niveles_activos = HashMap::new();

    let (i_uni, _) = sheets_lower
        .iter()
        .find_position(|a| a.contains("uni"))
        .unwrap();
    let (i_nivel, _) = sheets_lower
        .iter()
        .find_position(|a| a.contains("nivel"))
        .unwrap();

    let uni = xl.worksheet_range(&sheets[i_uni]).unwrap();
    let nivel = xl.worksheet_range(&sheets[i_nivel]).unwrap();

    for row in uni
        .rows()
        .filter(|a| a.first().is_some_and(|first| first.is_empty().not()))
    {
        match row {
            [Data::String(uni), Data::String(normalized), ..] => {
                universidades.insert(uni.to_string(), normalized.to_string());
            }
            _ => unreachable!(),
        }
    }

    for row in nivel
        .rows()
        .filter(|a| a.first().is_some_and(|first| first.is_empty().not()))
    {
        match row {
            [Data::String(nivel), Data::String(activo), ..] => {
                niveles_activos.insert(
                    nivel.to_string(),
                    ["si", "sí", "0"].contains(&activo.to_lowercase().as_str()),
                );
            }
            [Data::String(nivel), Data::Int(activo), ..] => {
                niveles_activos.insert(nivel.to_string(), *activo != 0);
            }
            [Data::String(nivel), Data::Float(activo), ..] => {
                niveles_activos.insert(nivel.to_string(), *activo != 0.);
            }
            _ => unreachable!(),
        }
    }

    Config {
        universidades,
        niveles_activos,
    }
}

fn main() {
    let Args {
        rutas,
        salida,
        config,
    } = Args::parse();

    let from_cli = !rutas.is_empty() || salida.is_some() || config.is_some();

    if from_cli && config.is_none() {
        eprintln!("No se ha especificado el archivo de universidades");
        std::process::exit(1);
    }
    let config = if let Some(config) = config {
        config
    } else {
        match ask_open_file(
            "Selecciona el archivo de configuration en Excel",
            Some(&[("Excel", &["xlsx"])]),
        ) {
            Some(path) => path,
            None => return,
        }
    };

    let config = read_config_xlsx(&config);

    if rutas.is_empty() && from_cli {
        eprintln!("No se han especificado rutas");
        std::process::exit(1);
    }
    let rutas = if rutas.is_empty() {
        match ask_open_files(
            "Selecciona los archivos de ANUIES a procesar",
            Some(&[("Excel", &["xlsx"])]),
        ) {
            Some(path) => path,
            None => return,
        }
    } else {
        rutas
    };

    if salida.is_none() && from_cli {
        eprintln!("No se ha especificado un archivo de salida");
        std::process::exit(1);
    }
    let output = if let Some(output) = salida {
        output
    } else {
        match ask_save_file(
            "Selecciona cómo almacenar el resultado",
            Some(&[("Excel", &["xlsx"])]),
        ) {
            Some(path) => path,
            None => return,
        }
    };

    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    let mut c_row = 1;

    let mut strings = HashSet::new();
    let headers = get_headers(rutas.first().unwrap());
    let headers = headers.as_slice();

    make_persistent_related(headers, &rutas, &mut strings);
    let relations = get_unique_relations(headers, &rutas, &strings);

    headers.iter().enumerate().for_each(|(i, v)| {
        worksheet.write(0, i.try_into().unwrap(), v).unwrap();
    });

    for ruta in &rutas {
        let mut xl = open_excel(ruta);
        let range = get_first_sheet(&mut xl);

        let mut rows = range.rows().skip(1);

        // Obtenemos el ciclo
        let ciclo = rows.next().unwrap().first().unwrap();
        let ciclo = if let Some(ciclo) = ciclo.get_string() {
            &ciclo.split_once("Ciclo escolar ").unwrap().1[0..9]
        } else {
            "NONE"
        };

        // Siempre hay una linea despues del título y el encabezado
        rows.next();
        rows.next();

        for row in rows {
            if row.contains(&DataRef::Empty) {
                continue;
            }

            // Normalizamos el nivel de la universidad
            let uni = get_uni_name(
                row[get_relevant_id(headers, UNI)].get_string().unwrap(),
                &config.universidades,
            );
            // Normalizamos el nivel del programa
            let nivel = get_nivel_name(row[get_relevant_id(headers, NIVEL)].get_string().unwrap());
            let campo_detallado = row[get_relevant_id(headers, CAMPO_DETALLADO)]
                .get_string()
                .unwrap();
            let campo_especifico = row[get_relevant_id(headers, CAMPO_ESPECIFICO)]
                .get_string()
                .unwrap();
            let campo_amplio = row[get_relevant_id(headers, CAMPO_AMPLIO)]
                .get_string()
                .unwrap();

            if !nivel_activo(nivel, &config.niveles_activos) {
                continue;
            }

            // No registramos datos de "Otras" (no competidores)
            if uni == "Otras" {
                continue;
            }

            worksheet.write(c_row, 0, ciclo).unwrap();
            for (c_col, v_col) in row.iter().enumerate() {
                let c_col: u16 = c_col.try_into().unwrap();
                match c_col {
                    col if get_relevant_id_u16(headers, UNI) == col => {
                        worksheet.write(c_row, c_col + 1, uni).unwrap();
                    }
                    col if get_relevant_id_u16(headers, NIVEL) == col => {
                        worksheet.write(c_row, c_col + 1, nivel).unwrap();
                    }
                    _ => match v_col {
                        DataRef::Int(v_col) => {
                            worksheet.write(c_row, c_col + 1, *v_col).unwrap();
                        }
                        DataRef::Float(v_col) => {
                            worksheet.write(c_row, c_col + 1, *v_col).unwrap();
                        }
                        DataRef::DateTime(v_col) => {
                            worksheet.write(c_row, c_col + 1, v_col.as_f64()).unwrap();
                        }
                        DataRef::Bool(v_col) => {
                            worksheet.write(c_row, c_col + 1, *v_col).unwrap();
                        }
                        DataRef::String(v_col)
                        | DataRef::DateTimeIso(v_col)
                        | DataRef::DurationIso(v_col) => {
                            worksheet.write(c_row, c_col + 1, v_col).unwrap();
                        }
                        &DataRef::SharedString(v_col) => {
                            worksheet.write(c_row, c_col + 1, v_col).unwrap();
                        }
                        DataRef::Error(v_col) => {
                            eprintln!("{v_col:?}");
                        }
                        DataRef::Empty => {}
                    },
                }
            }

            let len: u16 = headers.len().try_into().unwrap();
            worksheet
                .write(
                    c_row,
                    len - 1,
                    *relations
                        .get(&(campo_amplio, campo_detallado, campo_especifico, nivel))
                        .unwrap(),
                )
                .unwrap();

            c_row += 1;
        }
    }

    workbook.save(&output).unwrap();
    rfd::MessageDialog::new()
        .set_level(rfd::MessageLevel::Info)
        .set_title("Listo")
        .show();
}

fn make_persistent<'a>(strings: &'a mut HashSet<String>, text: &str) -> &'a str {
    if strings.contains(text) {
        strings.get(text).as_ref().unwrap()
    } else {
        strings.insert(text.to_string());
        strings.get(text).as_ref().unwrap()
    }
}

fn get_relevant_id(headers: &[String], name: &str) -> usize {
    headers
        .iter()
        .enumerate()
        .find(|a| a.1 == name)
        .map(|a| a.0)
        .unwrap()
        .checked_sub(1)
        .unwrap()
}

fn get_relevant_id_u32(headers: &[String], name: &str) -> u32 {
    get_relevant_id(headers, name).try_into().unwrap()
}

fn get_relevant_id_u16(headers: &[String], name: &str) -> u16 {
    get_relevant_id(headers, name).try_into().unwrap()
}

fn open_excel(ruta: &std::path::Path) -> calamine::Xlsx<std::io::BufReader<std::fs::File>> {
    match ruta.extension().and_then(std::ffi::OsStr::to_str) {
        Some("xlsx") => (),
        _ => panic!("Expecting an excel file"),
    }
    open_workbook(ruta).unwrap()
}

#[allow(dead_code)]
fn get_uni_name<'a>(raw: &str, config: &'a HashMap<String, String>) -> &'a str {
    config.get(raw.trim()).map_or("Otras", |a| a.as_str())
}

fn nivel_activo(raw: &str, config: &HashMap<String, bool>) -> bool {
    config.get(raw.trim()).is_some_and(|activo| *activo)
}

fn get_nivel_name(raw: &str) -> &str {
    match raw {
        "DOCTORADO" | "ESPECIALIDAD" | "MAESTRÍA" | "TÉCNICO SUPERIOR" => raw,
        "LICENCIATURA EN EDUCACIÓN NORMAL" | "LICENCIATURA UNIVERSITARIA Y TECNOLÓGICA" => {
            "LICENCIATURA"
        }
        _ => panic!("Nivel no válido: {raw}"),
    }
}
