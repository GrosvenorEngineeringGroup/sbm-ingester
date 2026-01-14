#!/bin/bash
# Deploy all SBM Ingester components
#
# Usage: ./scripts/deploy.sh [command] [options]
#
# Commands:
#   all              Deploy everything (default)
#   lambda           Deploy all Lambda functions only
#   lambda <name>    Deploy specific Lambda (ingester|redrive|nem12|archiver|glue-trigger)
#   glue             Deploy Glue script only
#   terraform        Run terraform apply only
#
# Options:
#   --skip-terraform  Skip terraform apply
#   --skip-lambda     Skip Lambda deployment
#   --skip-glue       Skip Glue script upload
#   --plan-only       Run terraform plan without apply
#   --auto-approve    Auto-approve terraform apply
#   -h, --help        Show this help message
#
# Examples:
#   ./scripts/deploy.sh                    # Deploy all
#   ./scripts/deploy.sh --skip-terraform   # Deploy Lambda + Glue only
#   ./scripts/deploy.sh lambda ingester    # Deploy ingester Lambda only
#   ./scripts/deploy.sh glue               # Deploy Glue script only
#   ./scripts/deploy.sh terraform          # Run terraform only

set -e

# Configuration
GLUE_SCRIPT_BUCKET="aws-glue-assets-318396632821-ap-southeast-2"
GLUE_SCRIPT_KEY="scripts/hudiImportScript"
GLUE_SCRIPT_LOCAL="src/glue/hudi_import/script.py"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Logging functions
log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_section() { echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"; echo -e "${CYAN}  $1${NC}"; echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}\n"; }

# Default flags
SKIP_TERRAFORM=false
SKIP_LAMBDA=false
SKIP_GLUE=false
PLAN_ONLY=false
AUTO_APPROVE=false
COMMAND="all"
LAMBDA_TARGET=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-terraform)
            SKIP_TERRAFORM=true
            shift
            ;;
        --skip-lambda)
            SKIP_LAMBDA=true
            shift
            ;;
        --skip-glue)
            SKIP_GLUE=true
            shift
            ;;
        --plan-only)
            PLAN_ONLY=true
            shift
            ;;
        --auto-approve)
            AUTO_APPROVE=true
            shift
            ;;
        -h|--help)
            sed -n '2,27p' "$0" | sed 's/^# //' | sed 's/^#//'
            exit 0
            ;;
        all|lambda|glue|terraform)
            COMMAND=$1
            shift
            # If lambda command, check for specific target
            if [[ "$COMMAND" == "lambda" && $# -gt 0 && ! "$1" =~ ^-- ]]; then
                LAMBDA_TARGET=$1
                shift
            fi
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# ================================
# Deployment Functions
# ================================

deploy_terraform() {
    log_section "Terraform Deployment"

    cd "$PROJECT_ROOT/iac"

    log_info "Initializing Terraform..."
    terraform init -upgrade -input=false > /dev/null

    log_info "Running terraform plan..."
    terraform plan -out=tfplan

    if [[ "$PLAN_ONLY" == "true" ]]; then
        log_warn "Plan only mode - skipping apply"
        return 0
    fi

    # Check if there are changes
    if terraform show -json tfplan | jq -e '.resource_changes | length == 0' > /dev/null 2>&1; then
        log_info "No infrastructure changes needed"
        return 0
    fi

    if [[ "$AUTO_APPROVE" == "true" ]]; then
        log_info "Applying changes (auto-approved)..."
        terraform apply tfplan
    else
        echo ""
        read -p "Apply these changes? [y/N] " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            terraform apply tfplan
        else
            log_warn "Terraform apply skipped"
        fi
    fi

    cd "$PROJECT_ROOT"
}

deploy_glue_script() {
    log_section "Glue Script Deployment"

    if [[ ! -f "$GLUE_SCRIPT_LOCAL" ]]; then
        log_error "Glue script not found: $GLUE_SCRIPT_LOCAL"
        exit 1
    fi

    log_info "Uploading Glue script to S3..."
    aws s3 cp "$GLUE_SCRIPT_LOCAL" "s3://$GLUE_SCRIPT_BUCKET/$GLUE_SCRIPT_KEY"

    log_info "Glue script deployed to s3://$GLUE_SCRIPT_BUCKET/$GLUE_SCRIPT_KEY"
}

deploy_lambda() {
    log_section "Lambda Deployment"

    if [[ -n "$LAMBDA_TARGET" ]]; then
        log_info "Deploying Lambda: $LAMBDA_TARGET"
        "$SCRIPT_DIR/deploy-lambda.sh" "$LAMBDA_TARGET"
    else
        log_info "Deploying all Lambda functions..."
        "$SCRIPT_DIR/deploy-lambda.sh" all
    fi
}

deploy_all() {
    local start_time=$(date +%s)

    echo ""
    echo -e "${CYAN}╔═══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║              SBM Ingester Full Deployment                     ║${NC}"
    echo -e "${CYAN}╚═══════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    # Step 1: Terraform
    if [[ "$SKIP_TERRAFORM" == "false" ]]; then
        deploy_terraform
    else
        log_warn "Skipping Terraform (--skip-terraform)"
    fi

    # Step 2: Glue Script
    if [[ "$SKIP_GLUE" == "false" ]]; then
        deploy_glue_script
    else
        log_warn "Skipping Glue script (--skip-glue)"
    fi

    # Step 3: Lambda Functions
    if [[ "$SKIP_LAMBDA" == "false" ]]; then
        deploy_lambda
    else
        log_warn "Skipping Lambda deployment (--skip-lambda)"
    fi

    # Summary
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    log_section "Deployment Complete"
    echo -e "  ${GREEN}Duration:${NC} ${duration}s"
    echo ""

    # Show what was deployed
    echo -e "  ${CYAN}Deployed components:${NC}"
    [[ "$SKIP_TERRAFORM" == "false" ]] && echo -e "    ${GREEN}✓${NC} Terraform infrastructure"
    [[ "$SKIP_GLUE" == "false" ]] && echo -e "    ${GREEN}✓${NC} Glue script (hudiImportScript)"
    [[ "$SKIP_LAMBDA" == "false" ]] && echo -e "    ${GREEN}✓${NC} Lambda functions (5)"
    echo ""

    # Show skipped components
    if [[ "$SKIP_TERRAFORM" == "true" || "$SKIP_GLUE" == "true" || "$SKIP_LAMBDA" == "true" ]]; then
        echo -e "  ${YELLOW}Skipped components:${NC}"
        [[ "$SKIP_TERRAFORM" == "true" ]] && echo -e "    ${YELLOW}○${NC} Terraform infrastructure"
        [[ "$SKIP_GLUE" == "true" ]] && echo -e "    ${YELLOW}○${NC} Glue script"
        [[ "$SKIP_LAMBDA" == "true" ]] && echo -e "    ${YELLOW}○${NC} Lambda functions"
        echo ""
    fi
}

# ================================
# Main
# ================================

case "$COMMAND" in
    all)
        deploy_all
        ;;
    terraform)
        deploy_terraform
        ;;
    glue)
        deploy_glue_script
        ;;
    lambda)
        deploy_lambda
        ;;
    *)
        log_error "Unknown command: $COMMAND"
        exit 1
        ;;
esac

log_info "Done!"
