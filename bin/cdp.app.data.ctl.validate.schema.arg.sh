#!/bin/bash

# Function to display usage instructions
usage() {
    echo "" >&2
    echo "Usage: $0 <schema_name>" >&2
    echo "  where <schema_name> is one of: cdp_app_01_dev_01, cdp_app_01_dev_02, cdp_app_01_staging, cdp_app_01_prod" >&2
    echo "" >&2
    echo "This utility refreshes all code related artifacts.  This means views and materialized views." >&2
    exit 1
}

# Validate the number of arguments
if [ "$#" -ne 1 ]; then
    usage
fi

# Assign the schema name from the argument
schema_name="$1"

# Validate the schema name
if [[ ! "$schema_name" =~ ^(cdp_app_01_dev_01|cdp_app_01_dev_02|cdp_app_01_staging|cdp_app_01_prod)$ ]]; then
    echo "Error: Invalid schema name provided." >&2
    usage
fi

# Return the validated schema name
echo "Validated schema: $schema_name" >&2       # Send info to stderr
echo "$schema_name"                             # This is the only output to stdout, intended for capture
