#!/bin/bash

# Check if schema_type is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <schema_type>"
    echo "schema_type values: dev, test, prod"
    exit 1
fi

SCHEMA_TYPE=$1

# Validate schema_type
if [[ "$SCHEMA_TYPE" != "dev" && "$SCHEMA_TYPE" != "test" && "$SCHEMA_TYPE" != "prod" ]]; then
    echo "Invalid schema_type: $SCHEMA_TYPE"
    echo "schema_type values: dev, test, prod"
    exit 1
fi

echo ">> "
echo ">> STARTING psql"
echo ">> "

echo ">> SCC_DATA_DB_HOST: $SCC_DATA_DB_HOST"
echo ">> SCC_DATA_DB_PORT: $SCC_DATA_DB_PORT"
echo ">> SCC_DATA_DB_NAME: $SCC_DATA_DB_NAME"
echo ">> SCC_DATA_DB_USER: $SCC_DATA_DB_USER"
echo ">> "

echo ">> PGPASSWORD: $PGPASSWORD"

psql    -h $SCC_DATA_DB_HOST    \
        -p $SCC_DATA_DB_PORT    \
        -d $SCC_DATA_DB_NAME    \
        -U $SCC_DATA_DB_USER    \
        -f $SCC_DATA_HOME_SCHEMA/cleanup/cleanup_${SCHEMA_TYPE}.sql
