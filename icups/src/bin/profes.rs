use calamine::Xlsx;
use itertools::izip;
use polars::{lazy::dsl::*, prelude::*};
use polars_excel_writer::PolarsXlsxWriter;
use polars_sheet_reader::{read_set_from_sheet, read_sheet, read_sheet_nth};
use std::{path::PathBuf, str::FromStr};

fn main() {
    let mut args = std::env::args();
    let ruta = if let Some(ruta) = args.nth(1) {
        PathBuf::from_str(&ruta).unwrap()
    } else {
        return;
    };

    let idiomas = if let Some(ruta) = args.next() {
        PathBuf::from_str(&ruta).unwrap()
    } else {
        return;
    };

    let personal = if let Some(ruta) = args.next() {
        PathBuf::from_str(&ruta).unwrap()
    } else {
        return;
    };

    let capacitados = if let Some(ruta) = args.next() {
        PathBuf::from_str(&ruta).unwrap()
    } else {
        return;
    };

    let config = if let Some(ruta) = args.next() {
        PathBuf::from_str(&ruta).unwrap()
    } else {
        return;
    };

    let df = read_sheet_nth::<_, Xlsx<_>>(ruta, 0).unwrap();
    let profes_idioma = read_sheet_nth::<_, Xlsx<_>>(idiomas, 0).unwrap();
    let direccion_personal = read_sheet_nth::<_, Xlsx<_>>(personal, 2).unwrap();
    let capacitados = read_sheet::<_, Xlsx<_>>(capacitados, "IG-3").unwrap();

    let area_mapper = read_set_from_sheet::<Xlsx<_>>(&config, "Uniques", false).unwrap();
    let ciudad_mapper = read_set_from_sheet::<Xlsx<_>>(&config, "Pais", false).unwrap();

    let direccion_personal = direccion_personal
        .lazy()
        .with_column(col("Ciudad o País").map(
            move |s| {
                Ok(Some(Series::from_iter(s.str().unwrap().into_iter().map(
                    |v| {
                        v.map(|s| {
                            ciudad_mapper
                                .get(s)
                                .map(|a| {
                                    // println!("{a:?}");
                                    a.as_str()
                                })
                                .unwrap_or_else(|| {
                                    println!("Value not found {s:?}");
                                    "OTRO"
                                })
                                .to_string()
                        })
                        .unwrap_or_default()
                    },
                ))))
            },
            GetOutput::from_type(DataType::String),
        ))
        .collect()
        .unwrap();

    // Convertimos los IDs en UInt64
    let (df, profes, direccion_personal, capacitados) = {
        let df = df
            .lazy()
            .with_columns(&[
                col("Id Profesor").cast(DataType::UInt64),
                col("Class Id").cast(DataType::UInt64),
                col("Id Curso").cast(DataType::UInt64),
            ])
            .collect()
            .unwrap();

        let direccion_personal = direccion_personal
            .lazy()
            .with_column(
                col("ID del profesor que cuenta con posgrado")
                    .str()
                    .strip_chars(lit(" "))
                    .cast(DataType::UInt64),
            )
            .select([
                col("ID del profesor que cuenta con posgrado"),
                col("Ciudad o País"),
                col("Último grado obtenido"),
            ])
            .collect()
            .unwrap();

        let capacitados = capacitados
            .lazy()
            .filter(
                col("Nombre(s) del profesor")
                    .str()
                    .strip_chars(lit(" "))
                    .str()
                    .len_chars()
                    .neq(lit(0)),
            )
            .select([
                col("Escuela o Facultad").alias("Grupo Académico"),
                col("Campus").alias("Institución"),
                col("Idioma diferente al español en que puede impartir clase"),
                col("Nombre(s) del profesor"),
                col("Apellido paterno del profesor"),
                col("Apellido materno de profesor"),
            ])
            .unique(None, UniqueKeepStrategy::First)
            .select([
                col("Grupo Académico"),
                col("Institución"),
                col("Idioma diferente al español en que puede impartir clase"),
            ])
            .collect()
            .unwrap();

        let profes_idioma = profes_idioma
            .lazy()
            .with_columns(&[
                col("Id Profesor")
                    .str()
                    .strip_chars(lit(" "))
                    .cast(DataType::UInt64),
                col("Class ID")
                    .str()
                    .strip_chars(lit(" "))
                    .cast(DataType::UInt64),
                col("ID Curso")
                    .str()
                    .strip_chars(lit(" "))
                    .cast(DataType::UInt64),
            ])
            .select([
                col("Id Profesor"),
                col("ID Curso"),
                col("Class ID"),
                col("Idioma"),
            ])
            .collect()
            .unwrap();

        (df, profes_idioma, direccion_personal, capacitados)
    };

    // let df = df.lazy().with_column(
    //     as_struct([""])
    // )

    let capacitados = capacitados
        .lazy()
        .group_by(&[col("Institución"), col("Grupo Académico")])
        .agg([
            col("Idioma diferente al español en que puede impartir clase")
                .filter(
                    col("Idioma diferente al español en que puede impartir clase")
                        .str()
                        .strip_chars(lit(" "))
                        .str()
                        .len_chars()
                        .neq(0),
                )
                .n_unique()
                .alias("PTC capacitados para impartir clases en inglés"),
        ])
        .collect()
        .unwrap();

    let df = df
        .join(
            &profes,
            &["Id Profesor", "Class Id", "Id Curso"],
            &["Id Profesor", "Class ID", "ID Curso"],
            JoinArgs::new(JoinType::Left),
        )
        .unwrap();

    let df = df
        .join(
            &direccion_personal,
            &["Id Profesor"],
            &["ID del profesor que cuenta con posgrado"],
            JoinArgs::new(JoinType::Left),
        )
        .unwrap();

    // let meds = read_set_from_sheet::<Xlsx<_>>(&config, "Medicina", false).unwrap();

    let df = df
        .lazy()
        .with_column(
            col("Área RRHH")
                .map(
                    move |s| {
                        Ok(Some(Series::from_iter(s.iter().map(|val| {
                            if let AnyValue::String(s) = val {
                                let v = if let Some(v) = area_mapper.get(s) {
                                    v
                                } else {
                                    println!("Warning: Llave no encontrada {s:#?}");
                                    "OTRO"
                                };

                                v.to_string()
                            } else {
                                "OTRO".to_string()
                            }
                        }))))
                    },
                    GetOutput::from_type(DataType::String),
                )
                .alias("C Área RRHH"),
        )
        .collect()
        .unwrap();

    let df = df
        .clone()
        .lazy()
        .with_columns(
            &[as_struct(vec![col("Grupo Académico"), col("C Área RRHH")])
                .apply(
                    move |s| {
                        let ca = s.struct_()?;
                        let s_grupo = &ca.fields()[0];
                        let s_mat = &ca.fields()[1];

                        let ca_grupo = s_grupo.str()?;
                        let ca_mat = s_mat.str()?;

                        let out: StringChunked = izip!(ca_grupo, ca_mat)
                            .map(|(grupo, area)| {
                                if grupo.is_some_and(|a| a == "CSAL")
                                    && area.is_some_and(|a| a.starts_with("CSAL"))
                                {
                                    area
                                } else {
                                    grupo
                                }
                            })
                            .collect();

                        Ok(Some(out.into_series()))
                    },
                    GetOutput::from_type(DataType::String),
                )
                .alias("Grupo Académico")],
        )
        .collect()
        .unwrap();

    // println!("{df:?}");

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

    let df2 = df
        .clone()
        .lazy()
        .select(&[
            col("Institución"),
            col("Grupo Académico"),
            // col("Grupo Académico 2"),
            col("C Área RRHH"),
            col("Área RRHH"),
            col("Id Profesor"),
            col("Tipo de contrato"),
            // col("Materia"),
        ])
        // .filter(col("Tipo de contrato").eq(lit("Planta")))
        .unique(None, UniqueKeepStrategy::First)
        .collect()
        .unwrap();

    write_xlsx(df2, "temps.xlsx");

    let df = df
        .lazy()
        .group_by(["Institución", "Grupo Académico"])
        .agg([
            // Todos los profesores que hay (ids únicos)
            col("Id Profesor")
                .n_unique()
                .alias("Total profesores que imparten clases en la Escuela o Facultad"),
            // Numero de profesores que son de timepo completo con grado de doctor en usa o europa
            col("Id Profesor")
                .filter(
                    col("Último grado obtenido")
                        .str()
                        .contains_literal(lit("octor")),
                )
                .filter(
                    col("Ciudad o País")
                        .eq(lit("USA"))
                        .or(col("Ciudad o País").eq(lit("UE"))),
                )
                .n_unique()
                .alias("Doctorados en Europa Y USA"),
            // Numero de profesores que tienen una clase marcada como ingles
            col("Id Profesor")
                .filter(
                    col("Idioma")
                        .str()
                        .strip_chars(lit(""))
                        .str()
                        .contains_literal(lit("INGLES")),
                )
                .n_unique()
                .alias("Profesores que imparten clases en inglés"),
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
                        .and(col("Grupo Académico").eq(col("C Área RRHH"))),
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
            JoinArgs::new(JoinType::Left),
        )
        .unwrap();

    let df = df
        .join(
            &capacitados,
            ["Institución", "Grupo Académico"],
            ["Institución", "Grupo Académico"],
            JoinArgs::new(JoinType::Left),
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
            (col("Doctorados en Europa Y USA").cast(DataType::Float64)
                / col("PTC Doctores").cast(DataType::Float64)
                * lit(100.0))
            .alias("% PTC con doctorados en Europa y USA"),
            (col("PTC que imparten clases en la Escuela o Facultad").cast(DataType::Float64)
                / col("Total profesores que imparten clases en la Escuela o Facultad")
                    .cast(DataType::Float64)
                * lit(100.0))
            .alias("% PTC imparten clases en inglés"),
            (col("PTC capacitados para impartir clases en inglés").cast(DataType::Float64)
                / col("PTC que imparten clases en la Escuela o Facultad").cast(DataType::Float64)
                * lit(100.0))
            .alias("% PTC capacitados para impartir clases en inglés"),
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
            col("% PTC con doctorados en Europa y USA"),
            col("% PTC imparten clases en inglés"),
            col("% PTC capacitados para impartir clases en inglés"),
            col("PTC Doctores"),
            col("Doctorados en Europa Y USA"),
            col("Profesores que imparten clases en inglés"),
            col("PTC capacitados para impartir clases en inglés"),
            col("Horas PTC"),
            col("Horas Totales"),
        ])
        .collect()
        .unwrap();

    let df = df
        .sort(["Institución", "Grupo Académico"], false, false)
        .unwrap();

    let df = df.fill_null(FillNullStrategy::Zero).unwrap();

    write_xlsx(df, "dataframe.xlsx");
}

fn write_xlsx(df: DataFrame, name: &str) {
    let mut writer = PolarsXlsxWriter::new();
    writer.write_dataframe(&df).unwrap();
    writer.save(name).unwrap();
}
