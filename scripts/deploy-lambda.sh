#!/bin/bash
# Deploy Lambda functions locally with Linux-compatible packages
# Usage: ./scripts/deploy-lambda.sh [function]
# Examples:
#   ./scripts/deploy-lambda.sh           # Deploy all functions
#   ./scripts/deploy-lambda.sh ingester  # Deploy only ingester
#   ./scripts/deploy-lambda.sh redrive   # Deploy only redrive

set -e

# Configuration
S3_BUCKET="gega-code-deployment-bucket"
S3_PREFIX="sbm-files-ingester"
PYTHON_VERSION="313"
PLATFORM="manylinux_2_28_x86_64"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Clean up temporary directories
cleanup() {
    rm -rf /tmp/lambda_build /tmp/lambda_packages
}

build_ingester() {
    log_info "Building ingester Lambda..."

    cleanup
    mkdir -p /tmp/lambda_build /tmp/lambda_packages

    # Export requirements (exclude editable install)
    log_info "Exporting dependencies..."
    uv export --no-dev --no-hashes -o /tmp/requirements.txt
    grep -v "^-e" /tmp/requirements.txt > /tmp/requirements_clean.txt

    # Download Linux-compatible wheels
    log_info "Downloading Linux wheels (platform: $PLATFORM)..."
    pip download \
        -r /tmp/requirements_clean.txt \
        -d /tmp/lambda_packages \
        --platform "$PLATFORM" \
        --python-version "$PYTHON_VERSION" \
        --only-binary=:all: \
        --quiet

    # Extract wheels
    log_info "Extracting packages..."
    for whl in /tmp/lambda_packages/*.whl; do
        unzip -qo "$whl" -d /tmp/lambda_build/
    done

    # Copy source code
    log_info "Copying source code..."
    cp -r src/* /tmp/lambda_build/

    # Create zip
    log_info "Creating zip..."
    rm -f ingester.zip
    (cd /tmp/lambda_build && zip -rq "$PROJECT_ROOT/ingester.zip" .)

    local size=$(du -h ingester.zip | cut -f1)
    log_info "Built ingester.zip ($size)"

    cleanup
}

build_redrive() {
    log_info "Building redrive Lambda..."

    mkdir -p /tmp/lambda_build
    rm -rf /tmp/lambda_build/*

    cp src/functions/redrive_handler/app.py /tmp/lambda_build/redrive.py

    rm -f redrive.zip
    (cd /tmp/lambda_build && zip -rq "$PROJECT_ROOT/redrive.zip" .)

    local size=$(du -h redrive.zip | cut -f1)
    log_info "Built redrive.zip ($size)"
}

build_nem12_mappings() {
    log_info "Building nem12-mappings Lambda..."

    mkdir -p /tmp/lambda_build
    rm -rf /tmp/lambda_build/*

    cp src/functions/nem12_exporter/app.py /tmp/lambda_build/nem12_mappings_to_s3.py

    rm -f nem12-mappings-to-s3.zip
    (cd /tmp/lambda_build && zip -rq "$PROJECT_ROOT/nem12-mappings-to-s3.zip" .)

    local size=$(du -h nem12-mappings-to-s3.zip | cut -f1)
    log_info "Built nem12-mappings-to-s3.zip ($size)"
}

build_weekly_archiver() {
    log_info "Building weekly-archiver Lambda..."

    cleanup
    mkdir -p /tmp/lambda_build /tmp/lambda_packages

    # Export requirements
    uv export --no-dev --no-hashes -o /tmp/requirements.txt
    grep -v "^-e" /tmp/requirements.txt > /tmp/requirements_clean.txt

    # Download Linux-compatible wheels
    log_info "Downloading Linux wheels..."
    pip download \
        -r /tmp/requirements_clean.txt \
        -d /tmp/lambda_packages \
        --platform "$PLATFORM" \
        --python-version "$PYTHON_VERSION" \
        --only-binary=:all: \
        --quiet

    # Extract wheels
    for whl in /tmp/lambda_packages/*.whl; do
        unzip -qo "$whl" -d /tmp/lambda_build/
    done

    # Copy source
    cp src/functions/weekly_archiver/app.py /tmp/lambda_build/

    rm -f weekly_archiver.zip
    (cd /tmp/lambda_build && zip -rq "$PROJECT_ROOT/weekly_archiver.zip" .)

    local size=$(du -h weekly_archiver.zip | cut -f1)
    log_info "Built weekly_archiver.zip ($size)"

    cleanup
}

upload_and_deploy() {
    local zip_file=$1
    local function_name=$2
    local s3_key="$S3_PREFIX/$zip_file"

    log_info "Uploading $zip_file to s3://$S3_BUCKET/$s3_key..."
    aws s3 cp "$zip_file" "s3://$S3_BUCKET/$s3_key"

    log_info "Updating Lambda function: $function_name..."
    local result=$(aws lambda update-function-code \
        --function-name "$function_name" \
        --s3-bucket "$S3_BUCKET" \
        --s3-key "$s3_key" \
        --publish \
        --output json)

    local version=$(echo "$result" | jq -r '.Version')
    local state=$(echo "$result" | jq -r '.State')

    log_info "Deployed $function_name (Version: $version, State: $state)"
}

deploy_ingester() {
    build_ingester
    upload_and_deploy "ingester.zip" "sbm-files-ingester"
}

deploy_redrive() {
    build_redrive
    upload_and_deploy "redrive.zip" "sbm-files-ingester-redrive"
}

deploy_nem12_mappings() {
    build_nem12_mappings
    upload_and_deploy "nem12-mappings-to-s3.zip" "sbm-files-ingester-nem12-mappings-to-s3"
}

deploy_weekly_archiver() {
    build_weekly_archiver
    upload_and_deploy "weekly_archiver.zip" "sbm-weekly-archiver"
}

deploy_all() {
    deploy_ingester
    deploy_redrive
    deploy_nem12_mappings
    deploy_weekly_archiver
}

# Main
case "${1:-all}" in
    ingester)
        deploy_ingester
        ;;
    redrive)
        deploy_redrive
        ;;
    nem12|nem12-mappings)
        deploy_nem12_mappings
        ;;
    archiver|weekly-archiver)
        deploy_weekly_archiver
        ;;
    all)
        deploy_all
        ;;
    *)
        echo "Usage: $0 [ingester|redrive|nem12-mappings|weekly-archiver|all]"
        exit 1
        ;;
esac

log_info "Done!"
