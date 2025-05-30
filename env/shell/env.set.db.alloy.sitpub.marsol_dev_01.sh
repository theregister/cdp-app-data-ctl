
echo ">>"
echo ">> SETTING ENVIRONMENT VARIABLES"
echo ">> ALLOYDB_SITPUB_MARSOL_DEV_01"
echo ">>"

export CDP_APP_DATA_CTL_DB_ENVNAME=alloydb_sitpub_marsol_dev_01
export CDP_APP_DATA_CTL_DB_HOST=localhost
export CDP_APP_DATA_CTL_DB_PORT=$CDP_APP_DATA_CTL_GCP_TUNNEL_PORT
export CDP_APP_DATA_CTL_DB_NAME=sitpub
export CDP_APP_DATA_CTL_DB_USER=marsol_dev_01
export CDP_APP_DATA_CTL_DB_PASSWORD=$HELPER_ENV_DB_PASSWORD_ALLOYDB_SITPUB_MARSOL_DEV_01
export PGPASSWORD=$CDP_APP_DATA_CTL_DB_PASSWORD

# for making helper-python/lib postgres utils to work
export HELPER_PYTHON_DB_HOST=$CDP_APP_DATA_CTL_DB_HOST
export HELPER_PYTHON_DB_PORT=$CDP_APP_DATA_CTL_DB_PORT
export HELPER_PYTHON_DB_NAME=$CDP_APP_DATA_CTL_DB_NAME
export HELPER_PYTHON_DB_USER=$CDP_APP_DATA_CTL_DB_USER
export HELPER_PYTHON_DB_PASSWORD=$CDP_APP_DATA_CTL_DB_PASSWORD

echo ">> =================================================================="
echo ">> CDP_APP_DATA_CTL_GCP_TUNNEL_PORT      = "$CDP_APP_DATA_CTL_GCP_TUNNEL_PORT
echo ">> CDP_APP_DATA_CTL_DB_ENVNAME           = "$CDP_APP_DATA_CTL_DB_ENVNAME
echo ">> CDP_APP_DATA_CTL_DB_HOST              = "$CDP_APP_DATA_CTL_DB_HOST
echo ">> CDP_APP_DATA_CTL_DB_PORT              = "$CDP_APP_DATA_CTL_DB_PORT
echo ">> CDP_APP_DATA_CTL_DB_NAME              = "$CDP_APP_DATA_CTL_DB_NAME
echo ">> CDP_APP_DATA_CTL_DB_USER              = "$CDP_APP_DATA_CTL_DB_USER
echo ">> CDP_APP_DATA_CTL_DB_PASSWORD          = "$CDP_APP_DATA_CTL_DB_PASSWORD
echo ">> PGPASSWORD                     = "$PGPASSWORD
echo ">> HELPER_PYTHON_DB_HOST          = "$HELPER_PYTHON_DB_HOST
echo ">> HELPER_PYTHON_DB_PORT          = "$HELPER_PYTHON_DB_PORT
echo ">> HELPER_PYTHON_DB_NAME          = "$HELPER_PYTHON_DB_NAME
echo ">> HELPER_PYTHON_DB_USER          = "$HELPER_PYTHON_DB_USER
echo ">> HELPER_PYTHON_DB_PASSWORD      = "$HELPER_PYTHON_DB_PASSWORD
echo ">> =================================================================="
