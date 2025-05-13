
source $SCC_DATA_HOME_ENV/shell/env.set.db.local.scc_data_001.cc.sh

command="scc.data.d.c.build.sh"
echo ">> $command"
eval $command

command="sleep 3"
echo ">> $command"
eval $command

command="scc.data.db.create.sh"
echo ">> $command"
eval $command

command="scc.data.db.schema.create.sh dev"
echo ">> $command"
eval $command

command="scc.data.db.schema.create.seed.data.sh dev"
echo ">> $command"
eval $command

command="scc.data.db.schema.report.sh dev"
echo ">> $command"
eval $command
