# Variables
IMAGE_NAME = dapcrtl
CONTAINER_NAME = dapcrtl-${USER}

# Build the Docker image
build:
	DOCKER_BUILDKIT=1 docker build -t $(IMAGE_NAME) --progress=plain .

# Start the container with the current directory mounted and open a bash shell
start-mount: build
	docker run --rm --name $(CONTAINER_NAME) -v $(PWD):/app -it $(IMAGE_NAME) bash

start-mount-server: build
	docker run --rm --name $(CONTAINER_NAME) -p8000:8000 -v $(PWD):/app -it $(IMAGE_NAME)  

# Clean up the container and image
clean:
	docker rm -f $(CONTAINER_NAME) || true
	docker rmi $(IMAGE_NAME) || true
	 
 