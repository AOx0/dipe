#!/usr/bin/fish
begin
    find . -name '*xlsx' | xargs -I {} fish -c "xlist -r select \"{}\" (xlist sheets \"{}\" 0 | sed 's/\"//g') -s \"{}.csv\""
    find . -name '*csv' | xargs -I {} ruplacer ' {2,}' ' ' {} --go --quiet
    find . -name '*csv' | xargs -I {} ruplacer ' ,' ',' {} --go --quiet
    find . -name '*csv' | xargs -I {} ruplacer ', ' ',' {} --go --quiet
end
