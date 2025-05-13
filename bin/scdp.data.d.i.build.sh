

# build new image
echo "DOCKER_IMAGE_TGT = "$DOCKER_IMAGE_TGT
echo "DOCKER_IMAGE_SRC = "$DOCKER_IMAGE_SRC

docker build -t $DOCKER_IMAGE_TGT .

# list all images
echo ">> "
echo ">> ================================================"
echo ">> docker image ls -a"
echo ">> ================================================"
echo ">> "
docker image ls -a

