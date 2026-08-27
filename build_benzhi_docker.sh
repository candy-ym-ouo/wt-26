#!/bin/bash
set -e

IMAGE_NAME=${1:-tsdb}
DOCKER_PLATFORM=${2:-linux/amd64}

docker build --platform "$DOCKER_PLATFORM" -f benzhi.Dockerfile -t "$IMAGE_NAME" .

echo ""
echo "Docker image '$IMAGE_NAME' built successfully!"
echo ""
echo "Next steps (for testing):"
echo "  Interactive shell: docker run -it $IMAGE_NAME:latest"
echo "  Run service: docker run --rm -p 8080:8080 $IMAGE_NAME:latest go run ./cmd/server -data-dir /tmp/tsdb -listen :8080"
