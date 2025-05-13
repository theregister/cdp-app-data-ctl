
source $SCC_DATA_HOME_ENV/shell/env.set.db.alloy.sitpub.cc.sh
source ~/MyFiles/MyProjects/helper-secrets/env/shell/env.set.secret.alloydb.sitpub.cc.pw.sh

command="scc.data.db.schema.cleanup.sh dev"
echo ">> $command"
eval $command

command="scc.data.db.schema.create.sh dev"
echo ">> $command"
eval $command

command="scc.data.db.schema.create.seed.data.sh dev"
echo ">> $command"
eval $command

# run generic to jump inside container
# d.c.connect.sh