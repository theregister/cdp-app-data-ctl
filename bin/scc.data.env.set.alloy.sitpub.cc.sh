
echo ""
echo "SOURCE THE FOLLOWING"
echo "source $SCC_DATA_HOME_ENV/shell/env.set.db.alloy.sitpub.cc.sh"
echo ""

tmp_dir=$SCDP_DATA_HOME_TMP/gcp.tunnel.start.sh
command='cat ${tmp_dir}/gcp.tunnel.start.out | grep -i "picking local unused port" | grep -o "[0-9]\+"'
PORT=$(eval "$command")
echo $PORT

echo "export SCC_DATA_DB_PORT="$PORT

echo "SCC_DATA_DB_HOST: "$SCC_DATA_DB_HOST
echo "SCC_DATA_DB_PORT: "$SCC_DATA_DB_PORT
echo "SCC_DATA_DB_NAME: "$SCC_DATA_DB_NAME
echo "SCC_DATA_DB_USER: "$SCC_DATA_DB_USER
echo "SCC_DATA_DB_PW: "$SCC_DATA_DB_PW
echo "PGPASSWORD: "$PGPASSWORD
echo ""