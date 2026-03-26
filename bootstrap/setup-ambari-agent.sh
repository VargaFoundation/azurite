#!/usr/bin/env bash
#
# setup-ambari-agent.sh - Install and configure Ambari Agent.
#
# This is the standalone equivalent of the cloud-init script for agent nodes
# (head-1, ZooKeeper nodes, and worker nodes). Run this manually if cloud-init
# failed or if you need to re-run the setup.
#
# Usage:
#   sudo ./setup-ambari-agent.sh <ambari-server-hostname> [OPTIONS]
#
# Arguments:
#   <ambari-server-hostname>   Hostname or FQDN of the Ambari server (head-0)
#
# Options:
#   -v, --ambari-version <version>   Ambari version (default: 2.7.5.0)
#   -h, --help                       Show this help message
#
set -euo pipefail

# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
AMBARI_VERSION="2.7.5.0"
AMBARI_SERVER_HOST=""

# --------------------------------------------------------------------------- #
# Colors
# --------------------------------------------------------------------------- #
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }

usage() {
    cat <<'USAGE'
Usage: sudo ./setup-ambari-agent.sh <ambari-server-hostname> [OPTIONS]

Install Ambari Agent and configure it to connect to the Ambari server.
This script must be run as root (or with sudo).

Arguments:
  <ambari-server-hostname>         Hostname or FQDN of the Ambari server (head-0)

Options:
  -v, --ambari-version <version>   Ambari version (default: 2.7.5.0)
  -h, --help                       Show this help message and exit

Examples:
  # Install agent pointing to head-0
  sudo ./setup-ambari-agent.sh azurite-head-0

  # Install a specific Ambari version
  sudo ./setup-ambari-agent.sh azurite-head-0 -v 2.7.5.0

USAGE
}

# --------------------------------------------------------------------------- #
# Parse arguments
# --------------------------------------------------------------------------- #

# First positional argument is the Ambari server hostname
if [[ $# -lt 1 ]]; then
    error "Missing required argument: <ambari-server-hostname>"
    echo ""
    usage
    exit 1
fi

# Check if first arg is a flag (help)
case "$1" in
    -h|--help)
        usage
        exit 0
        ;;
    -*)
        error "First argument must be the Ambari server hostname, got: $1"
        usage
        exit 1
        ;;
    *)
        AMBARI_SERVER_HOST="$1"
        shift
        ;;
esac

while [[ $# -gt 0 ]]; do
    case "$1" in
        -v|--ambari-version)
            AMBARI_VERSION="$2"
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
# Pre-flight checks
# --------------------------------------------------------------------------- #
if [[ $EUID -ne 0 ]]; then
    error "This script must be run as root. Use: sudo $0"
    exit 1
fi

if [[ -z "${AMBARI_SERVER_HOST}" ]]; then
    error "Ambari server hostname cannot be empty."
    exit 1
fi

info "Ambari version     : ${AMBARI_VERSION}"
info "Ambari server host : ${AMBARI_SERVER_HOST}"
echo ""

# --------------------------------------------------------------------------- #
# Step 1: Install Java 8
# --------------------------------------------------------------------------- #
info "Installing OpenJDK 8..."
apt-get update -qq
apt-get install -y -qq openjdk-8-jdk > /dev/null

# Set JAVA_HOME globally
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
if ! grep -q "JAVA_HOME" /etc/environment; then
    echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> /etc/environment
fi
success "Java 8 installed. JAVA_HOME=${JAVA_HOME}"

# --------------------------------------------------------------------------- #
# Step 2: Install Ambari agent
# --------------------------------------------------------------------------- #
info "Adding Ambari ${AMBARI_VERSION} repository..."

# Add Ambari repo
wget -nv "https://public-repo-1.hortonworks.com/ambari/ubuntu18/2.x/updates/${AMBARI_VERSION}/ambari.list" \
    -O /etc/apt/sources.list.d/ambari.list

# Import GPG key
apt-key adv --recv-keys --keyserver keyserver.ubuntu.com B9733A7A07513CAD 2>/dev/null || true

apt-get update -qq

info "Installing ambari-agent..."
apt-get install -y -qq ambari-agent > /dev/null
success "Ambari agent package installed."

# --------------------------------------------------------------------------- #
# Step 3: Configure agent to point to the Ambari server
# --------------------------------------------------------------------------- #
info "Configuring ambari-agent to point to '${AMBARI_SERVER_HOST}'..."

AGENT_INI="/etc/ambari-agent/conf/ambari-agent.ini"
if [[ -f "${AGENT_INI}" ]]; then
    sed -i "s/hostname=localhost/hostname=${AMBARI_SERVER_HOST}/" "${AGENT_INI}"
    success "Agent configured: hostname=${AMBARI_SERVER_HOST}"
else
    error "Agent config not found at ${AGENT_INI}"
    exit 1
fi

# --------------------------------------------------------------------------- #
# Step 4: Start the Ambari agent
# --------------------------------------------------------------------------- #
info "Starting ambari-agent..."
ambari-agent start
success "Ambari agent started."

echo ""
echo "============================================================"
echo "  Ambari Agent Setup Complete"
echo "============================================================"
echo ""
echo "  Agent is registered with server: ${AMBARI_SERVER_HOST}"
echo ""
echo "  Verify on the server:"
echo "    curl -u admin:admin http://${AMBARI_SERVER_HOST}:8080/api/v1/hosts"
echo ""
echo "  Service control:"
echo "    ambari-agent status"
echo "    ambari-agent restart"
echo ""
echo "  Logs:"
echo "    /var/log/ambari-agent/ambari-agent.log"
echo ""
echo "============================================================"
