#!/bin/bash
# Deploy CIM Exporter Lambda using Docker container image
#
# This script:
# 1. Builds the Docker image locally
# 2. Creates ECR repository if it doesn't exist
# 3. Authenticates with ECR
# 4. Tags and pushes the image to ECR
# 5. Updates the Lambda function with the new image
#
# Usage: ./scripts/deploy-cim-exporter.sh

set -euo pipefail

# Configuration
AWS_REGION="ap-southeast-2"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REPO_NAME="cim-exporter"
LAMBDA_FUNCTION_NAME="cim-report-exporter"
IMAGE_TAG="latest"

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKERFILE_DIR="$PROJECT_ROOT/src/functions/cim_exporter"

# ECR repository URI
ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}"

echo "=========================================="
echo "Deploying CIM Exporter Lambda"
echo "=========================================="
echo "AWS Account: $AWS_ACCOUNT_ID"
echo "Region: $AWS_REGION"
echo "ECR Repository: $ECR_REPO_NAME"
echo "Lambda Function: $LAMBDA_FUNCTION_NAME"
echo "=========================================="

# Step 1: Build Docker image
echo ""
echo "[1/5] Building Docker image..."
docker build -t "${ECR_REPO_NAME}:${IMAGE_TAG}" "$DOCKERFILE_DIR"

# Step 2: Create ECR repository if it doesn't exist
echo ""
echo "[2/5] Ensuring ECR repository exists..."
if ! aws ecr describe-repositories --repository-names "$ECR_REPO_NAME" --region "$AWS_REGION" > /dev/null 2>&1; then
    echo "Creating ECR repository: $ECR_REPO_NAME"
    aws ecr create-repository \
        --repository-name "$ECR_REPO_NAME" \
        --region "$AWS_REGION" \
        --image-scanning-configuration scanOnPush=true \
        --image-tag-mutability MUTABLE
else
    echo "ECR repository already exists"
fi

# Step 3: Authenticate with ECR
echo ""
echo "[3/5] Authenticating with ECR..."
aws ecr get-login-password --region "$AWS_REGION" | \
    docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# Step 4: Tag and push image
echo ""
echo "[4/5] Pushing image to ECR..."
docker tag "${ECR_REPO_NAME}:${IMAGE_TAG}" "${ECR_URI}:${IMAGE_TAG}"
docker push "${ECR_URI}:${IMAGE_TAG}"

# Step 5: Update Lambda function
echo ""
echo "[5/5] Updating Lambda function..."
aws lambda update-function-code \
    --function-name "$LAMBDA_FUNCTION_NAME" \
    --image-uri "${ECR_URI}:${IMAGE_TAG}" \
    --region "$AWS_REGION"

# Wait for update to complete
echo ""
echo "Waiting for Lambda update to complete..."
aws lambda wait function-updated \
    --function-name "$LAMBDA_FUNCTION_NAME" \
    --region "$AWS_REGION"

echo ""
echo "=========================================="
echo "Deployment completed successfully!"
echo "=========================================="
echo "Image URI: ${ECR_URI}:${IMAGE_TAG}"
echo ""
echo "To test the Lambda manually, run:"
echo "  aws lambda invoke --function-name $LAMBDA_FUNCTION_NAME --region $AWS_REGION /dev/stdout"
