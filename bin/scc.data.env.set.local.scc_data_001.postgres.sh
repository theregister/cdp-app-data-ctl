
echo $SCC_DATA_HOME_ENV

file_to_run=$SCC_DATA_HOME_ENV'/shell/env.set.db.local.scc_data_001.postgres.sh'
command='source '$file_to_run
echo ""
echo $file_to_run
ls -l $file_to_run
echo "RUN THIS COMMAND"
echo $command
echo ""

echo "SCC_DATA_DB_HOST: "$SCC_DATA_DB_HOST
echo "SCC_DATA_DB_PORT: "$SCC_DATA_DB_PORT
echo "SCC_DATA_DB_NAME: "$SCC_DATA_DB_NAME
echo "SCC_DATA_DB_USER: "$SCC_DATA_DB_USER
echo "SCC_DATA_DB_PW: "$SCC_DATA_DB_PW
echo "PGPASSWORD: "$PGPASSWORD
echo ""