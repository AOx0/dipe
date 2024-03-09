use calamine::Xlsx;
use polars::{lazy::dsl::*, prelude::*};
use polars_excel_writer::PolarsXlsxWriter;
use polars_sheet_reader::{read_set_from_sheet, read_sheet};
use std::{path::PathBuf, str::FromStr};

fn main() {
    let mut args = std::env::args();
    let ruta = if let Some(ruta) = args.nth(1) {
        PathBuf::from_str(&ruta).unwrap()
    } else {
        return;
    };

    let config = if let Some(ruta) = args.next() {
        PathBuf::from_str(&ruta).unwrap()
    } else {
        return;
    };

    let df = read_sheet::<_, Xlsx<_>>(ruta, "TITULARES").unwrap();
    let mapper = read_set_from_sheet::<Xlsx<_>>(&config, "Config", false).unwrap();

    let df = df
        .lazy()
        .with_column(col("Área RRHH").map(
            move |s| {
                Ok(Some(Series::from_iter(s.iter().map(|val| {
                    if let AnyValue::String(s) = val {
                        let v = if let Some(v) = mapper.get(s) {
                            v
                        } else {
                            println!("Warning: Llave no encontrada {s:?}");
                            "Otro"
                        };

                        v.to_string()
                    } else {
                        "Otro".to_string()
                    }
                }))))
            },
            GetOutput::from_type(DataType::String),
        ))
        .collect()
        .unwrap();

    let horas_profesor = df
        .clone()
        .lazy()
        .select([
            col("Institución"),
            col("Grupo Académico"),
            col("Id Profesor"),
            col("Tot Hrs Semana"),
            col("Tipo de contrato"),
        ])
        .unique(None, UniqueKeepStrategy::First)
        .group_by([
            "Institución",
            "Grupo Académico",
            "Id Profesor",
            // "Class Id",
            // "No. Clase",
        ])
        .agg([
            col("Tot Hrs Semana").first().alias("Horas Totales"),
            col("Tot Hrs Semana")
                .filter(col("Tipo de contrato").neq(lit("Asignatura")))
                .first()
                .alias("Horas PTC"),
        ])
        .group_by(["Institución", "Grupo Académico"])
        .agg([col("Horas PTC").sum(), col("Horas Totales").sum()])
        .collect()
        .unwrap();

    let df = df
        .lazy()
        .group_by(["Institución", "Grupo Académico"])
        .agg([
            // Todos los profesores que hay (ids únicos)
            col("Id Profesor")
                .n_unique()
                .alias("Total profesores que imparten clases en la Escuela o Facultad"),
            // Todos los profesores que tienen un contrato de tiempo completo
            col("Id Profesor")
                .filter(col("Tipo de contrato").neq(lit("Asignatura")))
                .n_unique()
                .alias("PTC que imparten clases en la Escuela o Facultad"),
            // Todos los profesores que dan clase que son de la facultad
            col("Id Profesor")
                .filter(
                    col("Tipo de contrato")
                        .neq(lit("Asignatura"))
                        .and(col("Grupo Académico").eq(col("Área RRHH"))),
                )
                .n_unique()
                .alias("PTC que pertenecen a la Escuela o Facultad y dan clases"),
            // Número de doctores (porque nivel estudio contiene .octor)
            col("Id Profesor")
                .filter(
                    col("Tipo de contrato").neq(lit("Asignatura")).and(
                        col("Último Nivel Estudio")
                            .str()
                            .contains_literal(lit("octor")),
                    ),
                )
                .n_unique()
                .alias("PTC Doctores"),
        ])
        .collect()
        .unwrap();

    let df = df
        .join(
            &horas_profesor,
            ["Institución", "Grupo Académico"],
            ["Institución", "Grupo Académico"],
            JoinArgs::new(JoinType::Inner),
        )
        .unwrap();

    let df = df
        .lazy()
        .with_columns([
            (col("PTC que imparten clases en la Escuela o Facultad")
                - col("PTC que pertenecen a la Escuela o Facultad y dan clases"))
            .alias("PTC de otras áreas que dan clases en la Escuela o Facultad"),
            ((col("PTC que imparten clases en la Escuela o Facultad").cast(DataType::Float64)
                / col("Total profesores que imparten clases en la Escuela o Facultad")
                    .cast(DataType::Float64))
                * lit(100.0))
            .alias("% PTC"),
            (col("PTC Doctores").cast(DataType::Float64)
                / col("PTC que imparten clases en la Escuela o Facultad").cast(DataType::Float64)
                * lit(100.0))
            .alias("% PTC con doctorado"),
            (col("Horas PTC").cast(DataType::Float64)
                / col("Horas Totales").cast(DataType::Float64)
                * lit(100))
            .alias("% hrs impartidas por PTC"),
        ])
        .collect()
        .unwrap();

    let df = df
        .lazy()
        .select([
            col("Institución"),
            col("Grupo Académico"),
            col("Total profesores que imparten clases en la Escuela o Facultad"),
            col("PTC que imparten clases en la Escuela o Facultad"),
            col("PTC que pertenecen a la Escuela o Facultad y dan clases"),
            col("PTC de otras áreas que dan clases en la Escuela o Facultad"),
            col("% PTC"),
            col("% hrs impartidas por PTC"),
            col("% PTC con doctorado"),
            // col("% doctorados en Europa y USA"),
            // col("% PTC imparten clases en inglés"),
            // col("% PTC capacitados para impartir clases en inglés"),
            col("PTC Doctores"),
            // col("Doctorados en Europa Y USA"),
            // col("Profesores que imparten clases en inglés"),
            // col("PTC capacitados para impartir clases en inglés"),
            col("Horas PTC"),
            col("Horas Totales"),
        ])
        .collect()
        .unwrap();

    let df = df
        .sort(["Institución", "Grupo Académico"], false, false)
        .unwrap();

    let mut writer = PolarsXlsxWriter::new();
    writer.write_dataframe(&df).unwrap();
    writer.save("dataframe.xlsx").unwrap();
}
