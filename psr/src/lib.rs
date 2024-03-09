#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![deny(rust_2018_idioms, unsafe_code)]
#![deny(clippy::unwrap_used)]

use ::strings::sanitize_spaces;
use calamine::{open_workbook, Data, DataType as _, Reader};
use polars::prelude::*;
use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    ops::Not,
    path::{Path, PathBuf},
};
use thiserror::Error;

const INT: u8 = 0b1000_0000;
const FLOAT: u8 = 0b0100_0000;
const STRING: u8 = 0b0010_0000;
const BOOL: u8 = 0b0001_0000;
const DATETIME: u8 = 0b0000_1000;
const EMPTY: u8 = 0b0000_0000;

fn ref_to_string(value: &Data) -> String {
    match value {
        Data::String(v) => v.to_owned(),
        _ => value.to_string(),
    }
}

/// Read a sheet with two columns as a `HashMap`<Column1, Column2>
///
/// # Panics
///
/// Panics if the sheet does not have two columns
///
/// # Errors
///
/// This function will return an error if theres an error opening the tabular file or while opening the sheet by name (i.e. a sheet with that name does not exist)
pub fn read_set_from_sheet<R: Reader<BufReader<File>>>(
    path: impl AsRef<Path>,
    sheet: &str,
    has_header: bool,
) -> ReaderResult<HashMap<String, String>> {
    let mut res = HashMap::new();
    let mut excel: R = open_workbook(path.as_ref())
        .map_err(|e| ReaderError::OpenWorkbook(path.as_ref().to_path_buf(), format!("{e:?}")))?;
    let range = excel
        .worksheet_range(sheet)
        .map_err(|e| ReaderError::OpenWorksheet(sheet.to_string(), format!("{e:?}")))?;

    let mut row = range.rows().map(|c| {
        if let [k, v, ..] = c {
            (k, v)
        } else {
            panic!("Expected a sheet with two columns")
        }
    });

    if has_header {
        row.next();
    }

    for (k, v) in row {
        let k = ref_to_string(k);
        let v = ref_to_string(v);

        let k = sanitize_spaces(k.trim());
        let v = sanitize_spaces(v.trim());

        res.insert(k, v);
    }

    Ok(res)
}

#[derive(Error, Debug)]
pub enum ReaderError {
    #[error("failed to open workbook at `{0:?}` with `{1}`")]
    OpenWorkbook(PathBuf, String),
    #[error("failed to open worksheet `{0:?}` with `{1}`")]
    OpenWorksheet(String, String),
    #[error("sheet `{0:?}` does not have headers")]
    NoHeaders(String),
    #[error("failed to add column `{0}` with `{1}`")]
    AddColumn(String, String),
}

pub type ReaderResult<T> = std::result::Result<T, ReaderError>;

/// Read all sheets from a path into a collection of `DataFrame`
///
/// # Errors
///
/// This function will return an error if theres an error opening sheets, workbooks or while
/// adding columns into dataframes
pub fn read_sheets<R, P>(path: P) -> ReaderResult<PlIndexMap<String, DataFrame>>
where
    R: Reader<BufReader<File>>,
    P: AsRef<Path>,
{
    let mut res = PlIndexMap::default();
    let mut excel: R = open_workbook(path.as_ref())
        .map_err(|e| ReaderError::OpenWorkbook(path.as_ref().to_path_buf(), format!("{e:?}")))?;
    let sheets = excel.sheet_names();

    for sheet in sheets {
        let df = read_sheet_from_sheets(&mut excel, &sheet)?;
        res.insert(sheet, df);
    }

    Ok(res)
}

/// Read a single sheet named `sheet` from the path `path`
///
/// # Errors
///
/// This function will return an error if theres an error opening sheets, workbooks or while
/// adding columns into dataframes
pub fn read_sheet<P, R>(path: P, sheet: &str) -> ReaderResult<DataFrame>
where
    R: Reader<BufReader<File>>,
    P: AsRef<Path>,
{
    let mut excel: R = open_workbook(path.as_ref())
        .map_err(|e| ReaderError::OpenWorkbook(path.as_ref().to_path_buf(), format!("{e:?}")))?;

    read_sheet_from_sheets(&mut excel, sheet)
}

/// Reads a single sheet into a dataframe, detecting de data type for each column
///
/// # Errors
///
/// This function will return an error if there is a problem adding columns because of different sizes,
///  if the sheet does not have headers, etc.
///
/// # Panics
///
/// The function panics if values can not be generalized to a single type, this is a bug and must be
/// reported
pub fn read_sheet_from_sheets<R: Reader<BufReader<File>>>(
    excel: &mut R,
    sheet: &str,
) -> ReaderResult<DataFrame> {
    let mut df = DataFrame::default();
    let range = excel
        .worksheet_range(sheet)
        .map_err(|e| ReaderError::OpenWorksheet(sheet.to_string(), format!("{e:?}")))?;

    let mut rows = range
        .rows()
        .filter(|row| row.first().is_some_and(|c| c.is_empty().not()));

    let Some(header_row) = rows.next() else {
        return Err(ReaderError::NoHeaders(sheet.to_string()));
    };

    let headers = header_row.iter().map(|d| {
        if let Data::String(s) = d {
            s.to_string()
        } else {
            d.to_string()
        }
    });

    for (n_col, header) in headers.enumerate() {
        let mut flags = EMPTY;
        let values = rows.clone().map(|row| &row[n_col]);

        for value in values.clone() {
            flags |= match value {
                Data::Int(_) => INT,
                Data::Float(_) => FLOAT,
                Data::String(_) | Data::DateTimeIso(_) | Data::DurationIso(_) | Data::Error(_) => {
                    STRING
                }
                Data::Bool(_) => BOOL,
                Data::DateTime(_) => DATETIME,
                Data::Empty => EMPTY,
            };
        }

        let flags = if (flags & STRING) == STRING {
            STRING
        } else if ((flags & INT) | (flags & FLOAT)) == (INT | FLOAT) {
            FLOAT
        } else {
            flags
        };

        let dtype = match flags {
            FLOAT => DataType::Float64,
            INT => DataType::Int64,
            BOOL => DataType::Boolean,
            DATETIME => DataType::Date,
            _ => DataType::String,
        };

        let mut vec_int64 = vec![];
        let mut vec_float64 = vec![];
        let mut vec_string = vec![];
        let mut vec_boolean = vec![];
        let mut vec_date = vec![];

        for value in values {
            populate_vectors(
                value,
                &dtype,
                &mut vec_int64,
                &mut vec_float64,
                &mut vec_string,
                &mut vec_boolean,
                &mut vec_date,
            );
        }

        assert_eq!(
            u8::from(vec_date.is_empty().not())
                + u8::from(vec_boolean.is_empty().not())
                + u8::from(vec_int64.is_empty().not())
                + u8::from(vec_float64.is_empty().not())
                + u8::from(vec_string.is_empty().not()),
            1
        );

        let series = match dtype {
            DataType::Boolean => Series::new(&header, vec_boolean),
            DataType::Int64 => Series::new(&header, vec_int64),
            DataType::Float64 => Series::new(&header, vec_float64),
            DataType::String => Series::new(&header, vec_string),
            DataType::Date => Series::new(&header, vec_date),
            _ => unreachable!(),
        };

        df.with_column(series)
            .map_err(|e| ReaderError::AddColumn(header, format!("{e:?}")))?;
    }

    Ok(df)
}

fn populate_vectors(
    value: &Data,
    dtype: &DataType,
    vec_int64: &mut Vec<Option<i64>>,
    vec_float64: &mut Vec<Option<f64>>,
    vec_string: &mut Vec<Option<String>>,
    vec_boolean: &mut Vec<Option<bool>>,
    vec_date: &mut Vec<Option<f64>>,
) {
    match value {
        Data::Int(v) if *dtype == DataType::Int64 => vec_int64.push(Some(*v)),
        Data::Float(v) if *dtype == DataType::Float64 => vec_float64.push(Some(*v)),
        Data::String(v) if *dtype == DataType::String => {
            vec_string.push(Some(::strings::sanitize_spaces(v)));
        }
        &Data::Bool(v) if *dtype == DataType::Boolean => vec_boolean.push(Some(v)),
        Data::DateTime(dur) if *dtype == DataType::Date => vec_date.push(Some(dur.as_f64())),
        Data::DateTimeIso(v) if *dtype == DataType::String => vec_string.push(Some(v.to_owned())),
        Data::DurationIso(v) if *dtype == DataType::String => vec_string.push(Some(v.to_owned())),
        Data::Error(_) => todo!(),
        _ => match *dtype {
            DataType::Boolean => vec_boolean.push(None),
            DataType::Int64 => vec_int64.push(None),
            DataType::Float64 => vec_float64.push(None),
            DataType::String => vec_string.push(None),
            DataType::Date => vec_date.push(None),
            _ => unreachable!("We must assign only one of the above"),
        },
    }
}
