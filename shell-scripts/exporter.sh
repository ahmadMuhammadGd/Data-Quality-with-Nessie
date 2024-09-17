

#!/bin/bash

config_dir=${1:-./config}

if [ -d "$config_dir" ]; then
    for envfile in "$config_dir"/*.env; do
        if [ -f "$envfile" ]; then
            echo "--------------------------------------------"
            echo "[$envfile]: Exporting variables"
            export $(grep -v '^\s*#' "$envfile" | xargs -d '\n')
            echo "[$envfile]: Done"
            echo "--------------------------------------------"
        else
            echo "No .env files found in $config_dir"
        fi
    done
else
    echo "Directory $config_dir does not exist."
fi