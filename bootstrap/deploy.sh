#!/usr/bin/env bash
#
# deploy.sh - Deploy the Azurite Ambari mpack cluster via Azure ARM template.
#
# This script validates and deploys the ARM template in this directory.
# It asks for confirmation before every destructive step.
#
# Usage:
#   ./deploy.sh [OPTIONS]
#
# Options:
#   -g, --resource-group   Resource group name (default: azurite-rg)
#   -l, --location         Azure region (default: eastus)
#   -p, --parameters       Path to parameters file (default: arm-parameters.example.json)
#   -n, --deployment-name  Deployment name (default: azurite-deploy-<timestamp>)
#   -h, --help             Show this help message
#
set -e

# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_FILE="${SCRIPT_DIR}/arm-template.json"
RESOURCE_GROUP="azurite-rg"
LOCATION="eastus"
PARAMETERS_FILE="${SCRIPT_DIR}/arm-parameters.example.json"
DEPLOYMENT_NAME="azurite-deploy-$(date +%Y%m%d%H%M%S)"

# --------------------------------------------------------------------------- #
# Colors for output
# --------------------------------------------------------------------------- #
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# --------------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------------- #
info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }

# Prompt for confirmation. Returns 0 if user says yes, 1 otherwise.
confirm() {
    local msg="${1:-Continue?}"
    echo -en "${YELLOW}${msg} [y/N]: ${NC}"
    read -r answer
    case "${answer}" in
        [yY]|[yY][eE][sS]) return 0 ;;
        *) return 1 ;;
    esac
}

usage() {
    cat <<'USAGE'
Usage: deploy.sh [OPTIONS]

Deploy the Azurite Ambari mpack cluster to Azure using an ARM template.

Options:
  -g, --resource-group <name>   Resource group name (default: azurite-rg)
  -l, --location <region>       Azure region (default: eastus)
  -p, --parameters <file>       Path to ARM parameters JSON file
                                (default: arm-parameters.example.json)
  -n, --deployment-name <name>  Deployment name
                                (default: azurite-deploy-<timestamp>)
  -h, --help                    Show this help message and exit

Examples:
  # Deploy with defaults (will prompt for confirmation at each step)
  ./deploy.sh

  # Deploy to a specific resource group and region
  ./deploy.sh -g my-rg -l westus2

  # Deploy with a custom parameters file
  ./deploy.sh -p my-params.json

USAGE
}

# --------------------------------------------------------------------------- #
# Parse arguments
# --------------------------------------------------------------------------- #
while [[ $# -gt 0 ]]; do
    case "$1" in
        -g|--resource-group)
            RESOURCE_GROUP="$2"
            shift 2
            ;;
        -l|--location)
            LOCATION="$2"
            shift 2
            ;;
        -p|--parameters)
            PARAMETERS_FILE="$2"
            shift 2
            ;;
        -n|--deployment-name)
            DEPLOYMENT_NAME="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# --------------------------------------------------------------------------- #
# Step 0: Verify that the ARM template and parameters files exist
# --------------------------------------------------------------------------- #
info "Template file : ${TEMPLATE_FILE}"
info "Parameters file: ${PARAMETERS_FILE}"
info "Resource group : ${RESOURCE_GROUP}"
info "Location       : ${LOCATION}"
info "Deployment name: ${DEPLOYMENT_NAME}"
echo ""

if [[ ! -f "${TEMPLATE_FILE}" ]]; then
    error "ARM template not found at: ${TEMPLATE_FILE}"
    exit 1
fi

if [[ ! -f "${PARAMETERS_FILE}" ]]; then
    error "Parameters file not found at: ${PARAMETERS_FILE}"
    exit 1
fi

# --------------------------------------------------------------------------- #
# Step 1: Check prerequisites - Azure CLI installed
# --------------------------------------------------------------------------- #
info "Checking prerequisites..."

if ! command -v az &>/dev/null; then
    error "Azure CLI (az) is not installed."
    error "Install it from: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
    exit 1
fi
success "Azure CLI is installed ($(az version --query '\"azure-cli\"' -o tsv))"

# Check that the user is logged in to Azure
if ! az account show &>/dev/null; then
    error "You are not logged in to Azure CLI."
    error "Run 'az login' first."
    exit 1
fi

ACCOUNT_NAME=$(az account show --query "name" -o tsv)
SUBSCRIPTION_ID=$(az account show --query "id" -o tsv)
success "Logged in to Azure subscription: ${ACCOUNT_NAME} (${SUBSCRIPTION_ID})"
echo ""

# --------------------------------------------------------------------------- #
# Step 2: Ensure the resource group exists (create if needed)
# --------------------------------------------------------------------------- #
info "Checking if resource group '${RESOURCE_GROUP}' exists..."

if az group show --name "${RESOURCE_GROUP}" &>/dev/null; then
    success "Resource group '${RESOURCE_GROUP}' already exists."
else
    warn "Resource group '${RESOURCE_GROUP}' does not exist."
    # Ask for confirmation before creating the resource group
    if ! confirm "Create resource group '${RESOURCE_GROUP}' in '${LOCATION}'?"; then
        error "Aborted. Cannot proceed without a resource group."
        exit 1
    fi
    info "Creating resource group '${RESOURCE_GROUP}' in '${LOCATION}'..."
    az group create \
        --name "${RESOURCE_GROUP}" \
        --location "${LOCATION}" \
        --output table
    success "Resource group created."
fi
echo ""

# --------------------------------------------------------------------------- #
# Step 3: Validate the ARM template
# --------------------------------------------------------------------------- #
info "Validating the ARM template..."

# Run az deployment group validate to catch errors before deploying
VALIDATION_OUTPUT=$(az deployment group validate \
    --resource-group "${RESOURCE_GROUP}" \
    --template-file "${TEMPLATE_FILE}" \
    --parameters "@${PARAMETERS_FILE}" \
    --output json 2>&1) || {
    error "Template validation failed:"
    echo "${VALIDATION_OUTPUT}"
    exit 1
}
success "ARM template validation passed."
echo ""

# --------------------------------------------------------------------------- #
# Step 4: Deploy the ARM template (with confirmation)
# --------------------------------------------------------------------------- #
info "Ready to deploy the Azurite cluster."
info "This will create the following resources in '${RESOURCE_GROUP}':"
info "  - 1 Virtual Network with 1 subnet"
info "  - 1 Network Security Group"
info "  - 1 ADLS Gen2 Storage Account with 'hadoop' container"
info "  - 1 User-Assigned Managed Identity + role assignment"
info "  - 2 Head Node VMs (Ambari server on head-0, agent on head-1)"
info "  - 3 ZooKeeper VMs"
info "  - N Worker VMs (as specified in parameters)"
echo ""

# Ask for explicit confirmation before deploying
if ! confirm "Proceed with deployment? This will create Azure resources and incur costs."; then
    warn "Deployment aborted by user."
    exit 0
fi

info "Starting deployment '${DEPLOYMENT_NAME}'..."
info "This may take 10-20 minutes. You can monitor progress in the Azure portal."
echo ""

# Deploy the ARM template
az deployment group create \
    --resource-group "${RESOURCE_GROUP}" \
    --name "${DEPLOYMENT_NAME}" \
    --template-file "${TEMPLATE_FILE}" \
    --parameters "@${PARAMETERS_FILE}" \
    --output table || {
    error "Deployment failed. Check the Azure portal for details:"
    error "  az deployment group show -g ${RESOURCE_GROUP} -n ${DEPLOYMENT_NAME} --query properties.error"
    exit 1
}

success "Deployment '${DEPLOYMENT_NAME}' completed successfully."
echo ""

# --------------------------------------------------------------------------- #
# Step 5: Retrieve and display deployment outputs
# --------------------------------------------------------------------------- #
info "Retrieving deployment outputs..."

AMBARI_URL=$(az deployment group show \
    --resource-group "${RESOURCE_GROUP}" \
    --name "${DEPLOYMENT_NAME}" \
    --query "properties.outputs.ambariUrl.value" -o tsv 2>/dev/null || echo "N/A")

SSH_COMMAND=$(az deployment group show \
    --resource-group "${RESOURCE_GROUP}" \
    --name "${DEPLOYMENT_NAME}" \
    --query "properties.outputs.sshCommand.value" -o tsv 2>/dev/null || echo "N/A")

STORAGE_ACCOUNT=$(az deployment group show \
    --resource-group "${RESOURCE_GROUP}" \
    --name "${DEPLOYMENT_NAME}" \
    --query "properties.outputs.storageAccountName.value" -o tsv 2>/dev/null || echo "N/A")

STORAGE_DFS=$(az deployment group show \
    --resource-group "${RESOURCE_GROUP}" \
    --name "${DEPLOYMENT_NAME}" \
    --query "properties.outputs.storageAccountDfs.value" -o tsv 2>/dev/null || echo "N/A")

IDENTITY_CLIENT_ID=$(az deployment group show \
    --resource-group "${RESOURCE_GROUP}" \
    --name "${DEPLOYMENT_NAME}" \
    --query "properties.outputs.managedIdentityClientId.value" -o tsv 2>/dev/null || echo "N/A")

echo ""
echo "============================================================"
echo "  Azurite Cluster Deployment Summary"
echo "============================================================"
echo ""
echo "  Ambari URL          : ${AMBARI_URL}"
echo "  SSH to head-0       : ${SSH_COMMAND}"
echo "  Storage Account     : ${STORAGE_ACCOUNT}"
echo "  ADLS Gen2 endpoint  : ${STORAGE_DFS}"
echo "  Managed Identity ID : ${IDENTITY_CLIENT_ID}"
echo ""
echo "  Default Ambari credentials: admin / admin"
echo ""
echo "  NOTE: It may take 5-10 minutes after deployment for"
echo "        cloud-init to finish installing Ambari on each VM."
echo "        Check cloud-init logs: /var/log/cloud-init-output.log"
echo ""
echo "============================================================"
