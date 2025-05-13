
echo ">> "
echo ">> STARTING psql"
echo ">> "

echo ">> CDP_DATA_CTL_DB_HOST: $CDP_DATA_CTL_DB_HOST"
echo ">> CDP_DATA_CTL_DB_PORT: $CDP_DATA_CTL_DB_PORT"
echo ">> CDP_DATA_CTL_DB_NAME: $CDP_DATA_CTL_DB_NAME"
echo ">> CDP_DATA_CTL_DB_USER: $CDP_DATA_CTL_DB_USER"
echo ">> "

echo ">> PGPASSWORD: $PGPASSWORD"

psql    -h $CDP_DATA_CTL_DB_HOST    \
        -p $CDP_DATA_CTL_DB_PORT    \
        -d $CDP_DATA_CTL_DB_NAME    \
        -U $CDP_DATA_CTL_DB_USER
