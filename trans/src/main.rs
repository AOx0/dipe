// Área de Estudio
// Posición ranking reforma 2021
// Posición ranking reforma 2022
// Posición ranking reforma 2023
// Cuota mercado 20-21
// Cuota mercado 21-22
// Cuota mercado 22-23
// Número estudiantes 2021
// Número estudiantes 2022
// Número estudiantes 2023
// CAGR matricula 18-21
// CAGR matrícula 21-22
// CAGR matrícula 20-23
// # profesores investigadores 2021
// # profesores investigadores 2022
// # profesores investigadores 2023
// Número de publicaciones ('17-'22)
// Número publicaciones ('18-'23)
// Número publicaciones ('19-'23)
// NPS 20-21
// NPS 21-22
// NPS 22-23
// Lugar NPS relativo 20-21
// Lugar NPS relativo 21-22
// Lugar NPS relativo 22-23
// Número de aplicaciones 20-21
// Número de aplicaciones 21-22
// Número de aplicaciones 22-23
// Aceptados/Aplicaciones 20-21
// Aceptados/Aplicaciones 21-22
// Aceptados/Aplicaciones 22-23
// Tasa matriculación (Inscritos/aceptados) 20-21
// Tasa matriculación (Inscritos/aceptados) 21-22
// Tasa matriculación (Inscritos/aceptados) 22-23
// Calificación 2020
// Calificación 2021
// Calificación 2022
// Sustenantes/total egresados 2021
// Sustenantes/total egresados 2022
// Sustenantes/total egresados 2023
// Satisfactorio/Sustentantes 20-21
// Satisfactorio/Sustentantes 21-22
// Satisfactorio/Sustentantes 22-23
// Relative Market Share 20-21
// Relative Market Share 21-22
// Relative Market Share 22-23
// Campus
// Orden

use polars::{io::SerReader, lazy::frame::IntoLazy};
use polars::{lazy::dsl::*, prelude::*};
use polars_excel_writer::PolarsXlsxWriter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let df = polars::prelude::CsvReader::from_path(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/evolucion.csv"
    ))?
    .has_header(true)
    .infer_schema(None)
    .finish()?;

    println!("Schema {:?}", df.schema());

    let df_2021 = df
        .clone()
        .lazy()
        .select([
            col("Área de Estudio")
                .alias("Área de Estudio")
                .cast(DataType::String),
            col("Posición ranking reforma 2021")
                .alias("Posición ranking reforma")
                .cast(DataType::String),
            col("Cuota mercado 20-21")
                .alias("Cuota mercado")
                .cast(DataType::String),
            col("Número estudiantes 2021")
                .alias("Número estudiantes")
                .cast(DataType::String),
            col("CAGR matricula 18-21")
                .alias("CAGR matrícula")
                .cast(DataType::String),
            col("# profesores investigadores 2021")
                .alias("# profesores investigadores")
                .cast(DataType::String),
            col("Número de publicaciones ('17-'22)")
                .alias("Número de publicaciones")
                .cast(DataType::String),
            col("NPS 20-21").alias("NPS").cast(DataType::String),
            col("Lugar NPS relativo 20-21")
                .alias("Lugar NPS relativo")
                .cast(DataType::String),
            col("Número de aplicaciones 20-21")
                .alias("Número de aplicaciones")
                .cast(DataType::String),
            col("Aceptados/Aplicaciones 20-21")
                .alias("Aceptados/Aplicaciones")
                .cast(DataType::String),
            col("Tasa matriculación (Inscritos/aceptados) 20-21")
                .alias("Tasa matriculación (Inscritos/aceptados)")
                .cast(DataType::String),
            col("Calificación 2020")
                .alias("Calificación")
                .cast(DataType::String),
            col("Sustenantes/total egresados 2021")
                .alias("Sustenantes/total egresados")
                .cast(DataType::String),
            col("Satisfactorio/Sustentantes 20-21")
                .alias("Satisfactorio/Sustentantes")
                .cast(DataType::String),
            col("Relative Market Share 20-21")
                .alias("Relative Market Share")
                .cast(DataType::String),
            col("Campus").alias("Campus").cast(DataType::String),
            col("Orden").alias("Orden").cast(DataType::String),
        ])
        .with_column(lit(2021).alias("Año"));

    let df_2022 = df
        .clone()
        .lazy()
        .select([
            col("Área de Estudio")
                .alias("Área de Estudio")
                .cast(DataType::String),
            col("Posición ranking reforma 2022")
                .alias("Posición ranking reforma")
                .cast(DataType::String),
            col("Cuota mercado 21-22")
                .alias("Cuota mercado")
                .cast(DataType::String),
            col("Número estudiantes 2022")
                .alias("Número estudiantes")
                .cast(DataType::String),
            col("CAGR matrícula 21-22")
                .alias("CAGR matrícula")
                .cast(DataType::String),
            col("# profesores investigadores 2022")
                .alias("# profesores investigadores")
                .cast(DataType::String),
            col("Número publicaciones ('18-'23)")
                .alias("Número de publicaciones")
                .cast(DataType::String),
            col("NPS 21-22").alias("NPS").cast(DataType::String),
            col("Lugar NPS relativo 21-22")
                .alias("Lugar NPS relativo")
                .cast(DataType::String),
            col("Número de aplicaciones 21-22")
                .alias("Número de aplicaciones")
                .cast(DataType::String),
            col("Aceptados/Aplicaciones 21-22")
                .alias("Aceptados/Aplicaciones")
                .cast(DataType::String),
            col("Tasa matriculación (Inscritos/aceptados) 21-22")
                .alias("Tasa matriculación (Inscritos/aceptados)")
                .cast(DataType::String),
            col("Calificación 2021")
                .alias("Calificación")
                .cast(DataType::String),
            col("Sustenantes/total egresados 2022")
                .alias("Sustenantes/total egresados")
                .cast(DataType::String),
            col("Satisfactorio/Sustentantes 21-22")
                .alias("Satisfactorio/Sustentantes")
                .cast(DataType::String),
            col("Relative Market Share 21-22")
                .alias("Relative Market Share")
                .cast(DataType::String),
            col("Campus").alias("Campus").cast(DataType::String),
            col("Orden").alias("Orden").cast(DataType::String),
        ])
        .with_column(lit(2022).alias("Año"));

    let df_2023 = df
        .clone()
        .lazy()
        .select([
            col("Área de Estudio")
                .alias("Área de Estudio")
                .cast(DataType::String),
            col("Posición ranking reforma 2023")
                .alias("Posición ranking reforma")
                .cast(DataType::String),
            col("Cuota mercado 22-23")
                .alias("Cuota mercado")
                .cast(DataType::String),
            col("Número estudiantes 2023")
                .alias("Número estudiantes")
                .cast(DataType::String),
            col("CAGR matrícula 20-23")
                .alias("CAGR matrícula")
                .cast(DataType::String),
            col("# profesores investigadores 2023")
                .alias("# profesores investigadores")
                .cast(DataType::String),
            col("Número publicaciones ('19-'23)")
                .alias("Número de publicaciones")
                .cast(DataType::String),
            col("NPS 22-23").alias("NPS").cast(DataType::String),
            col("Lugar NPS relativo 22-23")
                .alias("Lugar NPS relativo")
                .cast(DataType::String),
            col("Número de aplicaciones 22-23")
                .alias("Número de aplicaciones")
                .cast(DataType::String),
            col("Aceptados/Aplicaciones 22-23")
                .alias("Aceptados/Aplicaciones")
                .cast(DataType::String),
            col("Tasa matriculación (Inscritos/aceptados) 22-23")
                .alias("Tasa matriculación (Inscritos/aceptados)")
                .cast(DataType::String),
            col("Calificación 2022")
                .alias("Calificación")
                .cast(DataType::String),
            col("Sustenantes/total egresados 2023")
                .alias("Sustenantes/total egresados")
                .cast(DataType::String),
            col("Satisfactorio/Sustentantes 22-23")
                .alias("Satisfactorio/Sustentantes")
                .cast(DataType::String),
            col("Relative Market Share 22-23")
                .alias("Relative Market Share")
                .cast(DataType::String),
            col("Campus").alias("Campus").cast(DataType::String),
            col("Orden").alias("Orden").cast(DataType::String),
        ])
        .with_column(lit(2023).alias("Año"));

    let df_final =
        concat([df_2021, df_2022, df_2023].as_slice(), UnionArgs::default())?.collect()?;

    let df_final = df_final
        .lazy()
        .select([
            col("Campus"),
            col("Año"),
            col("Área de Estudio"),
            col("Posición ranking reforma").cast(DataType::String),
            col("Cuota mercado").cast(DataType::Float64),
            col("Número estudiantes").cast(DataType::UInt64),
            col("CAGR matrícula").cast(DataType::Float64),
            col("# profesores investigadores").cast(DataType::UInt64),
            col("Número de publicaciones").cast(DataType::UInt64),
            col("NPS").cast(DataType::Float64),
            col("Lugar NPS relativo").cast(DataType::UInt64),
            col("Número de aplicaciones").cast(DataType::UInt64),
            col("Aceptados/Aplicaciones").cast(DataType::Float64),
            col("Tasa matriculación (Inscritos/aceptados)").cast(DataType::Float64),
            col("Calificación").cast(DataType::UInt64),
            col("Sustenantes/total egresados").cast(DataType::Float64),
            col("Satisfactorio/Sustentantes").cast(DataType::Float64),
            col("Relative Market Share").cast(DataType::Float64),
            col("Orden").cast(DataType::UInt64),
        ])
        .collect()?;

    write_xlsx(&df_final, "procesado.xlsx")?;

    Ok(())
}

fn write_xlsx(df: &DataFrame, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = PolarsXlsxWriter::new();
    writer.set_autofit(true);
    writer.write_dataframe(df)?;
    writer.save(name)?;

    Ok(())
}
