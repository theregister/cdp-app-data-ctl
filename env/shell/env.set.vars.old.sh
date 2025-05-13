
# Detect shell and get script directory
if [ -n "$ZSH_VERSION" ]; then
    # ZSH
    echo ">> running in ZSH"
    SCRIPT_DIR=${0:A:h}
elif [ -n "$BASH_VERSION" ]; then
    # BASH
    echo ">> running in BASH"
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
else
    echo ">> running in other shell"
    # Fallback for other shells
    SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
fi

PROJECT_DIR=$(dirname "$(dirname "$SCRIPT_DIR")")

export CDP_DATA_CTL_HOME=$PROJECT_DIR

export CDP_DATA_CTL_HOME_BIN=$CDP_DATA_CTL_HOME/bin
export CDP_DATA_CTL_HOME_ENV=$CDP_DATA_CTL_HOME/env

export CDP_DATA_CTL_HOME_SCHEMA=$CDP_DATA_CTL_HOME/schema
export CDP_DATA_CTL_HOME_SQL=$CDP_DATA_CTL_HOME/sql
export CDP_DATA_CTL_HOME_DB=$CDP_DATA_CTL_HOME/db
export CDP_DATA_CTL_HOME_DATA=$CDP_DATA_CTL_HOME/data

export CDP_DATA_CTL_HOME_LOG=$CDP_DATA_CTL_HOME/log
export CDP_DATA_CTL_HOME_OUT=$CDP_DATA_CTL_HOME/out
export CDP_DATA_CTL_HOME_IN=$CDP_DATA_CTL_HOME/in
export CDP_DATA_CTL_HOME_TMP=$CDP_DATA_CTL_HOME/tmp

# by default connect to local postgres docker container
export CDP_DATA_CTL_DB_USER=postgres
export CDP_DATA_CTL_DB_PASSWORD=1234
export CDP_DATA_CTL_DB_HOST=localhost
export CDP_DATA_CTL_DB_PORT=5432
export CDP_DATA_CTL_DB_NAME=cdp_data_001

export CDP_DATA_CTL_GIT_SSH_ALIAS=

export CDP_DATA_CTL_OUTPUT_LEVEL=verbose

#export DOCKER_IMAGE_TGT=
#export DOCKER_IMAGE_TGT_DIR=
#export DOCKER_IMAGE_SRC=
#export DOCKER_CONTAINER=

# for making helper-python/lib postgres utils to work
export HELPER_PYTHON_DB_HOST=localhost
export HELPER_PYTHON_DB_PORT=5432
export HELPER_PYTHON_DB_NAME=cdp_data_001
export HELPER_PYTHON_DB_USER=marsol_dev_01
export HELPER_PYTHON_DB_PASSWORD=1234

# GCP Variables
export CDP_DATA_CTL_GCP_TUNNEL_PORT=

# add project bin to path
export PATH=$PATH:$CDP_DATA_CTL_HOME_BIN

export PYTHON_PATH_OLD=$PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$HOME/MyFiles/MyProjects/helper-python
export PYTHONPATH=$PYTHONPATH:$CDP_DATA_CTL_HOME/src/lib

export PGPASSWORD=1234
