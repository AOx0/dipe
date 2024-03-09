set -l commands sheets headers uniques help
set -l actions sheets headers uniques

function __fish_xlist_help_subcommand_completion
    set -l commands sheets headers uniques help
    set -l cmd_args (commandline -opc)

    if test (count $cmd_args) -eq 2
        echo $commands 2>/dev/null | tr " " "\n" || echo ""
    end
end

complete -c xlist -f

complete -c xlist -n "not __fish_seen_subcommand_from help" -s h -l help -d "Print help"

complete -c xlist -n "not __fish_seen_subcommand_from $commands" -a sheets -d "List available sheets in path"
complete -c xlist -n "not __fish_seen_subcommand_from $commands" -a headers -d "List available headers in a sheet from the specified path"
complete -c xlist -n "not __fish_seen_subcommand_from $commands" -a help -d "Print help of the given subcommand(s)"
complete -c xlist -n "not __fish_seen_subcommand_from $commands" -a uniques -d "Print unique values for the fiven columns from a sheet in a path"
complete -c xlist -n "__fish_seen_subcommand_from $actions" -F

# xlist help
complete -c xlist -f -n "__fish_seen_subcommand_from help" -a "(__fish_xlist_help_subcommand_completion)"
