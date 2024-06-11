use ::strings::{chars_to_lower, get_words, n_chars, replace_chars, rm_specials_char, space_join};
use itertools::Itertools;
use polars::{io::SerReader, lazy::frame::IntoLazy};
use polars::{lazy::dsl::*, prelude::*};
use polars_excel_writer::PolarsXlsxWriter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let df_vig = polars::prelude::CsvReader::from_path(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/Horas SNII/Padron_de_Investigadores_Vigentes_1T_2024.xlsx.csv"
    ))?
    .has_header(true)
    .infer_schema(None)
    .finish()?;

    let df_bnf = polars::prelude::CsvReader::from_path(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/Horas SNII/Padron_de_Beneficiarios_2023.xlsx.csv"
    ))?
    .has_header(true)
    .infer_schema(None)
    .finish()?;

    let df_p23 = polars::prelude::CsvReader::from_path(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/Horas SNII/titulares ago dic 23.xlsx.csv"
    ))?
    .has_header(true)
    .infer_schema(None)
    .finish()?;

    let df_p23 = df_p23
        .lazy()
        .with_columns([col("Nombre")
            .map(name_id(), GetOutput::from_type(DataType::String))
            .alias("SNombre")])
        .collect()?;

    let df_vig = df_vig
        .lazy()
        .with_columns([col("NOMBRE DEL INVESTIGADOR")
            .map(name_id(), GetOutput::from_type(DataType::String))
            .alias("SNombre")])
        .collect()?;

    let df_vig = df_vig
        .lazy()
        .select([
            col("CVU"),
            col("SNombre"),
            col("NOMBRE DEL INVESTIGADOR"),
            col("NIVEL"),
        ])
        .collect()?;

    let df_p23 = df_p23.join(
        &df_vig,
        ["SNombre"],
        ["SNombre"],
        JoinArgs::new(JoinType::Left),
    )?;

    println!("{:?}", df_vig.schema());
    println!("{:?}", df_bnf.schema());
    println!("{:?}", df_p23.schema());

    write_xlsx(&df_p23, "df_p23.xlsx")?;
    write_xlsx(&df_vig, "df_vig.xlsx")?;

    Ok(())
}

fn name_id() -> impl Fn(Series) -> Result<Option<Series>, PolarsError> {
    |s| {
        Ok(Some(Series::from_iter(s.str().unwrap().into_iter().map(
            |a| {
                let a = a.unwrap_or_default();

                // if let Some((apellidos, nombres)) = a.split_once(',') {
                //     let apellidos = clean_name(apellidos);
                //     let nombres = clean_name(nombres);

                //     String::from_iter(nombres.chain([' ']).chain(apellidos))
                // } else {
                //     println!("Warning with: {a:#?}");
                //     String::from_iter(clean_name(a))
                // }
                String::from_iter(clean_name(a))
            },
        ))))
    }
}

fn clean_name(name: &str) -> impl Iterator<Item = char> + '_ {
    // replace_chars(name.chars()).chunk_by()
    // name.chars().chu
    // chars_to_lower(rm_specials_char(n_chars(space_join(get_words(name)), 2)))
}

fn write_xlsx(df: &DataFrame, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = PolarsXlsxWriter::new();
    writer.write_dataframe(df)?;
    writer.save(name)?;

    Ok(())
}
