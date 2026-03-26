#!/usr/bin/env bash
#
# setup-ambari-server.sh - Install Ambari Server, PostgreSQL, and mpack on head-0.
#
# This is the standalone equivalent of the cloud-init script for head-0.
# Run this manually if cloud-init failed or if you need to re-run the setup.
#
# Usage:
#   sudo ./setup-ambari-server.sh [OPTIONS]
#
# Options:
#   -v, --ambari-version <version>   Ambari version (default: 2.7.5.0)
#   -m, --mpack-url <url>            URL to download the mpack tar.gz (optional)
#   -h, --help                       Show this help message
#
set -euo pipefail

# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
AMBARI_VERSION="2.7.5.0"
MPACK_URL=""

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
Usage: sudo ./setup-ambari-server.sh [OPTIONS]

Install Ambari Server, PostgreSQL, and optionally the Azurite mpack.
This script must be run as root (or with sudo).

Options:
  -v, --ambari-version <version>   Ambari version (default: 2.7.5.0)
  -m, --mpack-url <url>            URL to download the mpack tar.gz (optional)
  -h, --help                       Show this help message and exit

Examples:
  # Install with defaults
  sudo ./setup-ambari-server.sh

  # Install a specific Ambari version with mpack
  sudo ./setup-ambari-server.sh -v 2.7.5.0 -m https://example.com/mpack.tar.gz

USAGE
}

# --------------------------------------------------------------------------- #
# Parse arguments
# --------------------------------------------------------------------------- #
while [[ $# -gt 0 ]]; do
    case "$1" in
        -v|--ambari-version)
            AMBARI_VERSION="$2"
            shift 2
            ;;
        -m|--mpack-url)
            MPACK_URL="$2"
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

info "Ambari version : ${AMBARI_VERSION}"
info "Mpack URL      : ${MPACK_URL:-<none>}"
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
# Step 2: Install PostgreSQL and create ambari database
# --------------------------------------------------------------------------- #
info "Installing PostgreSQL..."
apt-get install -y -qq postgresql postgresql-contrib > /dev/null
systemctl enable postgresql
systemctl start postgresql
success "PostgreSQL installed and running."

info "Creating 'ambari' database and user..."

# Create the ambari user and database (idempotent)
sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='ambari'" | grep -q 1 || \
    sudo -u postgres psql -c "CREATE USER ambari WITH PASSWORD 'ambari';"

sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='ambari'" | grep -q 1 || \
    sudo -u postgres psql -c "CREATE DATABASE ambari OWNER ambari;"

sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE ambari TO ambari;"

# Update pg_hba.conf to allow password auth for local connections
PG_HBA=$(find /etc/postgresql -name pg_hba.conf | head -1)
if [[ -n "${PG_HBA}" ]]; then
    # Replace 'peer' with 'md5' for local connections
    sed -i 's/^local\s\+all\s\+all\s\+peer/local   all             all                                     md5/' "${PG_HBA}"
    systemctl restart postgresql
fi
success "PostgreSQL database 'ambari' created."

# --------------------------------------------------------------------------- #
# Step 3: Install Ambari server and agent
# --------------------------------------------------------------------------- #
info "Adding Ambari ${AMBARI_VERSION} repository..."

# Add Ambari repo
wget -nv "https://public-repo-1.hortonworks.com/ambari/ubuntu18/2.x/updates/${AMBARI_VERSION}/ambari.list" \
    -O /etc/apt/sources.list.d/ambari.list

# Import GPG key
apt-key adv --recv-keys --keyserver keyserver.ubuntu.com B9733A7A07513CAD 2>/dev/null || true

apt-get update -qq

info "Installing ambari-server and ambari-agent..."
apt-get install -y -qq ambari-server ambari-agent > /dev/null
success "Ambari server and agent packages installed."

# --------------------------------------------------------------------------- #
# Step 4: Setup Ambari server with PostgreSQL backend
# --------------------------------------------------------------------------- #
info "Running ambari-server setup..."

# Setup JDBC driver for PostgreSQL
ambari-server setup \
    --jdbc-db=postgres \
    --jdbc-driver=/usr/share/java/postgresql.jar \
    -s

# Run full setup with PostgreSQL as the backend database
ambari-server setup \
    -s \
    --java-home=/usr/lib/jvm/java-8-openjdk-amd64 \
    --database=postgres \
    --databasehost=localhost \
    --databaseport=5432 \
    --databasename=ambari \
    --databaseusername=ambari \
    --databasepassword=ambari

success "Ambari server setup complete."

# --------------------------------------------------------------------------- #
# Step 5: Install mpack if URL was provided
# --------------------------------------------------------------------------- #
if [[ -n "${MPACK_URL}" ]]; then
    info "Downloading mpack from: ${MPACK_URL}"
    wget -nv "${MPACK_URL}" -O /tmp/azurite-mpack.tar.gz

    info "Installing mpack..."
    ambari-server install-mpack --mpack=/tmp/azurite-mpack.tar.gz --verbose
    success "Mpack installed."
    rm -f /tmp/azurite-mpack.tar.gz
else
    info "No mpack URL provided, skipping mpack installation."
fi

# --------------------------------------------------------------------------- #
# Step 6: Start Ambari server and agent
# --------------------------------------------------------------------------- #
info "Starting ambari-server..."
ambari-server start
success "Ambari server started."

info "Configuring ambari-agent to point to localhost (head-0)..."
HOSTNAME_FQDN=$(hostname -f)
sed -i "s/hostname=localhost/hostname=${HOSTNAME_FQDN}/" /etc/ambari-agent/conf/ambari-agent.ini

info "Starting ambari-agent..."
ambari-agent start
success "Ambari agent started."

echo ""
echo "============================================================"
echo "  Ambari Server Setup Complete"
echo "============================================================"
echo ""
echo "  Ambari Web UI: http://$(hostname -f):8080"
echo "  Default credentials: admin / admin"
echo ""
echo "  Services:"
echo "    ambari-server status"
echo "    ambari-agent status"
echo ""
echo "  Logs:"
echo "    /var/log/ambari-server/ambari-server.log"
echo "    /var/log/ambari-agent/ambari-agent.log"
echo ""
echo "============================================================"
