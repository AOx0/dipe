use ::strings::{chars_to_lower, n_chars, rm_specials, rm_specials_char, space_join};
use itertools::Itertools;
use polars::{io::SerReader, lazy::frame::IntoLazy};
use polars::{lazy::dsl::*, prelude::*};
use polars_excel_writer::PolarsXlsxWriter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let vig_contents =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Vig.csv")).unwrap();
    let bnf_contents =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Bnf.csv")).unwrap();

    let vig_ids = vig_contents
        .lines()
        .map(str::trim)
        .collect::<std::collections::HashSet<&str>>();

    let bnf_ids = bnf_contents
        .lines()
        .map(str::trim)
        .collect::<std::collections::HashSet<&str>>();

    let missing_in_bnf = bnf_ids
        .difference(&vig_ids)
        .map(|a| a.parse::<u64>().unwrap())
        .collect_vec();

    println!("Values from bnf missing in vig: {}", missing_in_bnf.len());
    println!(
        "Total unique values: {}",
        bnf_ids
            .union(&vig_ids)
            .copied()
            .collect::<std::collections::HashSet<&str>>()
            .len()
    );

    let missing_in_bnf = Series::new("ID", missing_in_bnf);

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

    let df_vig = df_vig
        .lazy()
        .select([
            col("CVU"),
            col("NOMBRE DEL INVESTIGADOR"),
            col("NIVEL"),
            col("FECHA FIN DE VIGENCIA")
                .cast(DataType::UInt64)
                .cast(DataType::Date)
                .alias("FECHA DE FIN DE VIGENCIA"),
        ])
        .collect()?;

    let df_bnf = df_bnf
        .lazy()
        .filter(col("CVU").cast(DataType::UInt64).is_in(lit(missing_in_bnf)))
        .select([
            col("CVU"),
            col("NOMBRE DEL INVESTIGADOR"),
            col("NIVEL"),
            col("FECHA DE FIN DE VIGENCIA")
                .cast(DataType::UInt64)
                .cast(DataType::Date),
        ])
        .collect()?;

    println!("BNF LEN: {}", df_bnf.height());
    println!("VIG {:?}", df_vig.schema());
    println!("BNF {:?}", df_bnf.schema());

    let df_vig_bnf = concat(
        &[df_bnf.lazy(), df_vig.lazy()],
        UnionArgs {
            parallel: true,
            rechunk: true,
            to_supertypes: true,
        },
    )?
    .collect()?;

    println!("FINAL LEN: {}", df_vig_bnf.height());

    let df_vig_bnf = df_vig_bnf
        .lazy()
        .with_columns([col("NOMBRE DEL INVESTIGADOR")
            .map(name_id(), GetOutput::from_type(DataType::String))
            .alias("SNombre")])
        .collect()?;

    let df_p23 = df_p23
        .lazy()
        .with_columns([col("Nombre")
            .map(name_id(), GetOutput::from_type(DataType::String))
            .alias("SNombre")])
        .collect()?;

    let df_p23 = df_p23.join(
        &df_vig_bnf,
        ["SNombre"],
        ["SNombre"],
        JoinArgs::new(JoinType::Left),
    )?;

    println!("P23 {:?}", df_p23.schema());

    write_xlsx(&df_p23, "df_p23.xlsx")?;
    write_xlsx(&df_vig_bnf, "df_vig_bnf.xlsx")?;

    let df_p23_unicos = df_p23
        .lazy()
        .select([
            col("Id Profesor"),
            col("CVU"),
            col("Nombre"),
            col("NOMBRE DEL INVESTIGADOR").alias("Nombre Padron"),
            col("NIVEL").alias("Nivel"),
            col("FECHA DE FIN DE VIGENCIA").alias("Fin de vigencia"),
        ])
        .unique_stable(None, UniqueKeepStrategy::First)
        .with_column(col(""))
        .sort("Id Profesor", SortOptions::default())
        .collect()?;

    write_xlsx(&df_p23_unicos, "df_p23_uicos.xlsx")?;

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
    writer.write_dataframe(df)?;
    writer.save(name)?;

    Ok(())
}
