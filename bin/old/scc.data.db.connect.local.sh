
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
        -U $SCC_DATA_DB_USER
