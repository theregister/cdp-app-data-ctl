
#source secret env variables
source $HELPER_SECRETS_HOME_ENV/shell/scdp_data/env.set.secret.alloydb.sitpub.marsol_dev_01.pw.sh

# set SCC_DATA tunnel port
export SCC_DATA_GCP_TUNNEL_PORT=65499

# set SCC_DATA_ variables
export SCC_DATA_DB_HOST=localhost
export SCC_DATA_DB_PORT=$SCC_DATA_GCP_TUNNEL_PORT
export SCC_DATA_DB_NAME=sitpub
export SCC_DATA_DB_USER=marsol_dev_01
export SCC_DATA_DB_PASSWORD=$HELPER_SECRETS_PW

# set PGPASSWORD
export PGPASSWORD=$HELPER_SECRETS_PW

# set python library specific environment variables to match.
# need to figure out a better way to handle this.
export HELPER_PYTHON_DB_HOST=$SCC_DATA_DB_HOST
export HELPER_PYTHON_DB_PORT=$SCC_DATA_DB_PORT
export HELPER_PYTHON_DB_NAME=$SCC_DATA_DB_NAME
export HELPER_PYTHON_DB_USER=$SCC_DATA_DB_USER
export HELPER_PYTHON_DB_PASSWORD=$HELPER_SECRETS_PW

echo ""
echo "SCC_DATA_GCP_TUNNEL_PORT: "$SCC_DATA_GCP_TUNNEL_PORT
echo ""
echo "SCC_DATA_DB_HOST: "$SCC_DATA_DB_HOST
echo "SCC_DATA_DB_PORT: "$SCC_DATA_DB_PORT
echo "SCC_DATA_DB_NAME: "$SCC_DATA_DB_NAME
echo "SCC_DATA_DB_USER: "$SCC_DATA_DB_USER
echo "SCC_DATA_DB_PASSWORD: "$SCC_DATA_DB_PASSWORD
echo "PGPASSWORD: "$PGPASSWORD
echo ""