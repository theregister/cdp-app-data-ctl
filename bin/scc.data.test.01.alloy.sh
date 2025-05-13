
#!/usr/bin/env bash

# general settings
source $HOME/MyFiles/MyProjects/sitpub-content-classification-data/env/shell/env.set.sh
# override for sitpub content classification
source $SCC_DATA_HOME_ENV/shell/env.set.db.alloy.sitpub.cc.sh
# set secret
source ~/MyFiles/MyProjects/helper-secrets/env/shell/env.set.secret.alloydb.sitpub.cc.pw.sh

# db settings
scc.data.build.alloy.sh

scc.data.ctl.sh
scc.data.ctl.sh --help

#scc.data.ctl.sh extract



