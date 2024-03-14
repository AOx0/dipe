set -l commands sheets headers uniques help
set -l actions sheets headers uniques

function __fish_xlist_contains_sheets
    set -l cmd_args (commandline -opc)
    set -l contains n
    set -l list_arr headers uniques

    for arg in $cmd_args
        if contains -- $arg $list_arr
            set contains s
        end
    end

    echo $contains
end

function __fish_xlist_complete_sheets
    set -l contains (__fish_xlist_contains_sheets)

    if [ "$contains" = s ]
        set -l cmd_args (commandline -opc)
        xlist complete-sheets "$cmd_args[3]" 2>/dev/null || echo ""
    end
end

function __fish_xlist_complete_headers
    set -l contains (__fish_xlist_contains_sheets)

    if [ "$contains" = s ]
        set -l cmd_args (commandline -opc)
        xlist complete-headers "$cmd_args[3]" "$cmd_args[4]" 2>/dev/null || echo ""
    end
end

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
complete -c xlist -n "__fish_seen_subcommand_from $actions; and __fish_is_nth_token 2" -F

# xlist headers
complete -c xlist -n "__fish_seen_subcommand_from headers; and not __fish_seen_subcommand_from help; and __fish_is_nth_token 3" -ka '(__fish_xlist_complete_sheets)'

# xlist uniques
complete -c xlist -n "__fish_seen_subcommand_from uniques; and not __fish_seen_subcommand_from help; and __fish_is_nth_token 3" -ka '(__fish_xlist_complete_sheets)'
complete -c xlist -n "__fish_seen_subcommand_from uniques; and not __fish_seen_subcommand_from help; and not __fish_is_nth_token 3; and not __fish_is_nth_token 2" -ka '(__fish_xlist_complete_headers)'


# xlist help
complete -c xlist -f -n "__fish_seen_subcommand_from help" -a "(__fish_xlist_help_subcommand_completion)"
