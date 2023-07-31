#!/bin/zsh

# Check if image name is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <image_name>"
    exit 1
fi


# Stop all running Docker containers
docker stop $(docker ps -q)

# Remove all Docker containers
docker rm $(docker ps -a -q)

# Remove all Docker images
docker rmi $(docker images -q)

rm -rf "./output/output.csv"
rm -rf "./output/output-pathmatics.csv"
rm -rf "./output/output-vivvix.csv"

# Assign the first input argument to the IMAGE_NAME variable
IMAGE_NAME=$1

# Build the Docker image
docker build -t ${IMAGE_NAME} .

# Run the Docker image
docker run -v $(pwd)/input:/usr/src/app/input -v $(pwd)/output:/usr/src/app/output ${IMAGE_NAME}
