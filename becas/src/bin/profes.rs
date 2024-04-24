use polars::{lazy::dsl::*, prelude::*};
use polars_excel_writer::PolarsXlsxWriter;
use std::{path::PathBuf, str::FromStr};

fn main() {
    let mut args = std::env::args();
    let ruta = if let Some(ruta) = args.nth(1) {
        PathBuf::from_str(&ruta).unwrap()
    } else {
        return;
    };

    let df = polars::prelude::CsvReader::from_path(ruta)
        .unwrap()
        .has_header(true)
        .infer_schema(None)
        .finish()
        .unwrap();

    let df = df
        .lazy()
        .group_by(["Institución", "Grado Académico", "Ciclo", "ID"])
        .agg([col("Porcentaje de beca").sum().alias("Porcentaje total")])
        .filter(col("Porcentaje total").gt_eq(lit(100)))
        .group_by(["Institución", "Grado Académico", "Ciclo"])
        .agg([col("ID").count().alias("IDs")])
        .sort_by_exprs(
            vec![col("Institución"), col("Grado Académico"), col("Ciclo")],
            vec![true, true, true],
            false,
            false,
        )
        .collect()
        .unwrap();

    write_xlsx(df, "dataframe.xlsx");
}

fn write_xlsx(df: DataFrame, name: &str) {
    let mut writer = PolarsXlsxWriter::new();
    writer.write_dataframe(&df).unwrap();
    writer.save(name).unwrap();
}
