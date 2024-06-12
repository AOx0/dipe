use ::strings::{chars_to_lower, rm_specials, space_join};
use polars::{io::SerReader, lazy::frame::IntoLazy};
use polars::{lazy::dsl::*, prelude::*};
use polars_excel_writer::PolarsXlsxWriter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Generando 2023...");
    generar(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/Horas SNII/Padron_de_Beneficiarios_2023.xlsx.csv"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/Horas SNII/titulares ago dic 23.xlsx.csv"
        ),
        "df_pfs_23.xlsx",
        "df_bnf_23.xlsx",
        "df_uni_23.xlsx",
    )?;

    println!("Generando 2024...");
    generar(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/Horas SNII/Padron_de_Investigadores_Vigentes_1T_2024.xlsx.csv"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/Horas SNII/Titulares ene jul 24.xlsx.csv"
        ),
        "df_pfs_24.xlsx",
        "df_bnf_24.xlsx",
        "df_uni_24.xlsx",
    )?;

    Ok(())
}

fn generar(
    path_bnf: &str,
    path_profs: &str,
    nombre_pfs: &str,
    nombre_bnf: &str,
    nombre: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let df_bnf = polars::prelude::CsvReader::from_path(path_bnf)?
        .has_header(true)
        .infer_schema(None)
        .finish()?;

    let df_pfs = polars::prelude::CsvReader::from_path(path_profs)?
        .has_header(true)
        .infer_schema(None)
        .finish()?;

    let df_bnf = df_bnf
        .lazy()
        .select([
            col("CVU"),
            col("NOMBRE DEL INVESTIGADOR"),
            col("NIVEL"),
            col("FECHA FIN DE VIGENCIA").cast(DataType::UInt64),
        ])
        .with_columns([col("NOMBRE DEL INVESTIGADOR")
            .map(name_id(), GetOutput::from_type(DataType::String))
            .alias("SNombre")])
        .collect()?;

    let df_pfs = df_pfs
        .lazy()
        .with_columns([col("Nombre")
            .map(name_id(), GetOutput::from_type(DataType::String))
            .alias("SNombre")])
        .collect()?;

    let df_pfs = df_pfs.join(
        &df_bnf,
        ["SNombre"],
        ["SNombre"],
        JoinArgs::new(JoinType::Left),
    )?;

    write_xlsx(&df_pfs, nombre_pfs)?;
    write_xlsx(&df_bnf, nombre_bnf)?;

    let df_pfs_unicos = df_pfs
        .lazy()
        .select([
            col("Id Profesor"),
            col("CVU"),
            col("Institución"),
            col("Nombre"),
            // col("NOMBRE DEL INVESTIGADOR").alias("Nombre Padron"),
            col("NIVEL").alias("Nivel"),
            // Toca cambiar el nombre de la columna en uno de los dos
            col("FECHA FIN DE VIGENCIA").alias("Fin de vigencia"),
        ])
        .unique_stable(None, UniqueKeepStrategy::First)
        .with_column(
            col("Nivel")
                .map(
                    |s| {
                        Ok(Some(Series::from_iter(s.str().unwrap().into_iter().map(
                            |a| {
                                if a.is_none() || a.is_some_and(|a| a.is_empty()) {
                                    "".to_string()
                                } else {
                                    "No".to_string()
                                }
                            },
                        ))))
                    },
                    GetOutput::from_type(DataType::String),
                )
                .alias("Documento Vigente"),
        )
        .sort("Id Profesor", SortOptions::default())
        .collect()?;

    write_xlsx(&df_pfs_unicos, nombre)?;

    Ok(())
}

fn name_id() -> impl Fn(Series) -> Result<Option<Series>, PolarsError> {
    |s| {
        Ok(Some(Series::from_iter(s.str().unwrap().into_iter().map(
            |a| String::from_iter(clean_name(a.unwrap_or_default())),
        ))))
    }
}

fn clean_name(name: &str) -> impl Iterator<Item = char> + '_ {
    chars_to_lower(rm_specials(space_join(
        name.split(|a| a == ',' || a == ' '),
    )))
}

fn write_xlsx(df: &DataFrame, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = PolarsXlsxWriter::new();
    writer.set_autofit(true);
    writer.write_dataframe(df)?;
    writer.save(name)?;

    Ok(())
}
