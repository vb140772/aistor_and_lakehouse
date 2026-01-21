#!/bin/bash
# Build Docker image locally for x86 and push to Artifact Registry
# Usage: ./build_and_push_image.sh [image_tag]

set -e  # Exit on error

PROJECT_ID="infrastructure-433717"
REGION="us-central1"
IMAGE_NAME="duckdb-analysis"
IMAGE_TAG="${1:-latest}"
FULL_IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${IMAGE_NAME}/${IMAGE_NAME}:${IMAGE_TAG}"

echo "========================================="
echo "Building and pushing Docker image"
echo "========================================="
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Image: ${FULL_IMAGE_NAME}"
echo "Architecture: linux/amd64 (x86)"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "✗ Docker daemon is not running!"
    echo "Please start Docker and try again."
    exit 1
fi

# Configure Docker to use gcloud as credential helper for Artifact Registry
echo "Configuring Docker authentication..."
gcloud auth configure-docker ${REGION}-docker.pkg.dev --quiet

# Create Artifact Registry repository if it doesn't exist
echo "Ensuring Artifact Registry repository exists..."
gcloud artifacts repositories create ${IMAGE_NAME} \
  --repository-format=docker \
  --location=${REGION} \
  --project=${PROJECT_ID} \
  --description="DuckDB analysis container images" \
  2>/dev/null || {
    echo "Repository already exists or creation failed (continuing...)"
}

# Build the image for x86 architecture
echo "Building Docker image for linux/amd64..."
docker buildx build \
  --platform linux/amd64 \
  -t ${FULL_IMAGE_NAME} \
  -f docker/Dockerfile \
  --load \
  .

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Image built successfully!"
    
    # Push the image
    echo "Pushing image to Artifact Registry..."
    docker push ${FULL_IMAGE_NAME}
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "✓ Image pushed successfully!"
        echo "Image available at: ${FULL_IMAGE_NAME}"
    else
        echo ""
        echo "✗ Image push failed!"
        exit 1
    fi
else
    echo ""
    echo "✗ Image build failed!"
    exit 1
fi
