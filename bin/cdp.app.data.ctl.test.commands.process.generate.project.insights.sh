
#!/bin/bash

echo ">> ============================================"
echo ">> $(basename $0) - START"
echo ">> ============================================"

# exit immediately if a command exits with a non-zero status
set -e

# ensure schema correct
validated_schema=$(cdp.app.data.ctl.validate.schema.arg.sh "$@")

# general settings
source $CDP_APP_DATA_CTL_HOME_ENV/shell/env.set.sh

# override for alloydb connection
source $CDP_APP_DATA_CTL_HOME_ENV/shell/env.set.db.alloy.sitpub."$validated_schema".sh

command="cdp.app.data.ctl.sh process generate-project-insights"
echo ">> $command"
eval $command

echo ">> ============================================"
echo ">> $(basename $0) - END"
echo ">> ============================================"
echo ">> "
