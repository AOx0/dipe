#!/usr/bin/fish
begin
    # Para todos los exceles
    for ruta in (find . -type f -name '*[^s].xlsx')
        # Seleccionamos las columnas que nos interesan
        # Pero solo si el tipo de ayuda es de Colegiatura o Pagos 2 al 6
        xlist select $ruta "Becas por programa" \
            | xsv search -s 1 '.+' \
            | xsv search -s 20 '^(Pagos 2 al 6|Colegiatura|Pagos 02 al 06) *$' \
            | xsv select 1,3,9,15,20,22 >"$ruta.csv"
        # Quitamos espacios extras
        ruplacer ' {2,}' ' ' "$ruta.csv" --go --quiet
        # Y espacios entre valores
        ruplacer ' ,' ',' "$ruta.csv" --go --quiet
        # Nos deshacemos de la columna extra de tipo ayuda
        cat "$ruta.csv" | xsv select 1,2,3,4,6 >"$ruta.csv2"
        # Y lo hacemos efectivo en el archivo original
        mv "$ruta.csv2" "$ruta.csv"
    end
end
