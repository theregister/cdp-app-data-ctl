

clear

env_prefix="SCC_"
command_prefix="scc."

echo ">> "
echo ">> ================================================"
echo ">> HELP - START"
echo ">> ================================================"

echo ">> ======================"
echo ">> env variables"
echo ">> ======================"
command="env | grep $env_prefix | sort"
echo ">> "
echo ">> "$command
echo ">> "
eval $command



echo ">> ======================"
echo ">> commands"
echo ">> ======================"
command="ls -l $SCC_DATA_HOME_BIN/$command_prefix*"
echo ">> "
echo ">> $command"
echo ">> "
eval $command

echo ">> "
echo ">> ================================================"
echo ">> HELP - END"
echo ">> ================================================"
echo ">> "