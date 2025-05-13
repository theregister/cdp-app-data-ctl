
# project home
export CDP_DATA_CTL_HOME=$HOME/MyFiles/MyProjects/cdp-data-ctl

# standard variables
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
export CDP_DATA_CTL_DB_NAME=postgres

export CDP_DATA_CTL_GIT_SSH_ALIAS=github_sitpub

export CDP_DATA_CTL_DEGUG=false
export CDP_DATA_CTL_OUTPUT_LEVEL=verbose

export DOCKER_IMAGE_TGT=i-cdp-data-001
export DOCKER_IMAGE_SRC=.
export DOCKER_CONTAINER=c-cdp-data-001
# obsolete this.  very confusing!!
export DOCKER_CONTAINER_NAME=c-cdp-data-001

# for making helper-python/lib postgres utils to work
export HELPER_PYTHON_DB_HOST=localhost
export HELPER_PYTHON_DB_PORT=5432
export HELPER_PYTHON_DB_NAME=CDP_DATA_CTL_001
export HELPER_PYTHON_DB_USER=marsol_dev_01
export HELPER_PYTHON_DB_PASSWORD=1234

# add bin directory to path
export PATH=$PATH:$CDP_DATA_CTL_HOME_BIN

# include standard library from Sandbox-Python.
# There are commands in Infrastructure that are built with python.
export PYTHON_PATH_OLD=$PYTHONPATH
# start with helper-python lib
export PYTHONPATH=$HOME/MyFiles/MyProjects/helper-python
# 
export PYTHONPATH=$PYTHONPATH:$CDP_DATA_CTL_HOME/src/lib

export PGPASSWORD=1234

export CDP_DATA_CTL_GCP_TUNNEL_PORT=$CDP_DATA_CTL_GCP_TUNNEL_PORT