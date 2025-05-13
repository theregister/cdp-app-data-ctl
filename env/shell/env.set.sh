
echo ">> "
echo ">> ************************************************"
echo ">> CDP_DATA env - SET - START"
echo ">> ************************************************"
echo ">> "

# Detect shell and get script directory
if [ -n "$ZSH_VERSION" ]; then
    # ZSH
    echo ">> running in ZSH"
    PRIMARY_SCRIPT_DIR=${0:A:h}
elif [ -n "$BASH_VERSION" ]; then
    # BASH
    echo ">> running in BASH"
    PRIMARY_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
else
    echo ">> running in other shell"
    # Fallback for other shells
    PRIMARY_SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
fi

echo ">> set - environment variables"
source $PRIMARY_SCRIPT_DIR/env.set.vars.sh

echo ">> source secrets file"
source $PRIMARY_SCRIPT_DIR/env.set.secrets.sh

echo ">> aliases"
source $PRIMARY_SCRIPT_DIR/env.set.alias.sh

echo ">> to get started run the following..."
echo ">> cdp.data.help"
echo ">> "
echo ">> ************************************************"
echo ">> CDP_DATA_ env - SET - END"
echo ">> ************************************************"
echo ">> "
