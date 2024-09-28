

#!/bin/bash

config_dir=${1:-./config}

if [ -d "$config_dir" ]; then
    found_files=false
    for envfile in "$config_dir"/*.env; do
        if [ -f "$envfile" ]; then
            found_files=true
            echo "--------------------------------------------"
            echo "[$envfile]: Exporting variables"
            while IFS= read -r line || [[ -n "$line" ]]; do
                # Skip comments and empty lines
                if [[ ! "$line" =~ ^\s*#.*$ ]] && [[ -n "$line" ]]; then
                    # Remove leading/trailing whitespace
                    line=$(echo "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
                    # Export the variable
                    export "$line"
                    echo "Exported: $line"
                fi
            done < "$envfile"
            echo "[$envfile]: Done"
            echo "--------------------------------------------"
        fi
    done
    if [ "$found_files" = false ]; then
        echo "No .env files found in $config_dir"
    fi
else
    echo "Directory $config_dir does not exist."
fi
