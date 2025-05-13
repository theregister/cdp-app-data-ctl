
# generic docker container cleanup
command="d.c.cleanup.sh $DOCKER_CONTAINER"
echo ">> $command"
eval $command

# generic docker image cleanup
command="d.i.cleanup.sh $DOCKER_IMAGE_TGT"
echo ">> $command"
eval $command

# generic docker build
command="d.i.build.sh"
echo ">> $command"
eval $command

command="scc.data.dc.up.sh"
echo ">> $command"
eval $command

command="docker container ls -a"
echo ">> $command"
eval $command
