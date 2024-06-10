use polars::{lazy::dsl::*, prelude::*};
use polars_excel_writer::PolarsXlsxWriter;
use std::{path::PathBuf, str::FromStr};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args();
    let ruta = if let Some(ruta) = args.nth(1) {
        PathBuf::from_str(&ruta)?
    } else {
        return Ok(());
    };

    let df = polars::prelude::CsvReader::from_path(ruta)?
        .has_header(true)
        .infer_schema(None)
        .finish()?;

    let df = df
        .lazy()
        .group_by(["Institución", "Grado Académico", "ID"])
        .agg([col("Porcentaje de beca").sum().alias("Porcentaje total")])
        .collect()?;
    write_xlsx(&df, "suma_todos.xlsx")?;

    let df = df
        .lazy()
        .filter(col("Porcentaje total").gt_eq(lit(200)))
        .collect()?;
    write_xlsx(&df, "solo_200s.xlsx")?;

    let df = df
        .lazy()
        .group_by(["Institución", "Grado Académico"])
        .agg([col("ID").count().alias("IDs")])
        .sort_by_exprs(
            vec![col("Institución"), col("Grado Académico")],
            vec![true, true],
            false,
            false,
        )
        .collect()?;

    write_xlsx(&df, "dataframe.xlsx")?;

    Ok(())
}

fn write_xlsx(df: &DataFrame, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = PolarsXlsxWriter::new();
    writer.write_dataframe(df)?;
    writer.save(name)?;

    Ok(())
}
