#!/bin/bash
#
# OpenEndpoint One-Click Setup Script
# Supports: Linux, macOS
# Usage: ./setup.sh [docker|binary]
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
VERSION="1.0.0"
INSTALL_DIR="/opt/openendpoint"
DATA_DIR="/var/lib/openendpoint"
CONFIG_DIR="/etc/openendpoint"
LOG_DIR="/var/log/openendpoint"
SERVICE_USER="openendpoint"

# Detect OS and Architecture
 detect_os() {
    OS="$(uname -s)"
    ARCH="$(uname -m)"

    case "$OS" in
        Linux*)     PLATFORM="linux";;
        Darwin*)    PLATFORM="darwin";;
        CYGWIN*|MINGW*|MSYS*) PLATFORM="windows";;
        *)          echo "${RED}Unsupported OS: $OS${NC}"; exit 1;;
    esac

    case "$ARCH" in
        x86_64)     ARCH="amd64";;
        amd64)      ARCH="amd64";;
        arm64)      ARCH="arm64";;
        aarch64)    ARCH="arm64";;
        *)          echo "${RED}Unsupported architecture: $ARCH${NC}"; exit 1;;
    esac

    echo "${BLUE}Detected: $PLATFORM/$ARCH${NC}"
}

# Print banner
print_banner() {
    echo "${BLUE}"
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║                                                            ║"
    echo "║              OpenEndpoint Setup Script                     ║"
    echo "║              S3-Compatible Object Storage                  ║"
    echo "║                                                            ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo "${NC}"
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Generate random string
generate_random() {
    openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 32
}

# Create directories
setup_directories() {
    echo "${BLUE}Setting up directories...${NC}"
    sudo mkdir -p "$INSTALL_DIR/bin" "$DATA_DIR" "$CONFIG_DIR" "$LOG_DIR"

    # Create user if not exists
    if ! id "$SERVICE_USER" &>/dev/null; then
        echo "${BLUE}Creating service user...${NC}"
        sudo useradd -r -s /bin/false "$SERVICE_USER"
    fi

    sudo chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR" "$DATA_DIR" "$LOG_DIR"
    sudo chmod 755 "$INSTALL_DIR" "$DATA_DIR" "$CONFIG_DIR" "$LOG_DIR"
}

# Docker installation
install_docker() {
    echo "${BLUE}Installing OpenEndpoint with Docker...${NC}"

    if ! command_exists docker; then
        echo "${YELLOW}Docker not found. Installing Docker...${NC}"
        curl -fsSL https://get.docker.com | sh
        sudo usermod -aG docker "$USER"
        echo "${YELLOW}Please log out and log back in for Docker permissions to take effect.${NC}"
        exit 0
    fi

    # Create docker-compose.yml
    cat > docker-compose.yml << 'EOF'
version: '3.8'

services:
  openendpoint:
    image: openendpoint/openendpoint:1.0.0
    container_name: openendpoint
    ports:
      - "9000:9000"
    environment:
      - OPENEP_AUTH_ACCESS_KEY=${ACCESS_KEY:-minioadmin}
      - OPENEP_AUTH_SECRET_KEY=${SECRET_KEY:-minioadmin}
      - OPENEP_SERVER_HOST=0.0.0.0
      - OPENEP_SERVER_PORT=9000
      - OPENEP_STORAGE_DATA_DIR=/data
    volumes:
      - openendpoint-data:/data
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s

volumes:
  openendpoint-data:
EOF

    # Generate random credentials if not set
    if [ -z "$ACCESS_KEY" ]; then
        export ACCESS_KEY=$(generate_random | head -c 16)
        export SECRET_KEY=$(generate_random | head -c 32)
    fi

    # Start services
    docker-compose up -d

    echo "${GREEN}✓ OpenEndpoint installed with Docker${NC}"
    echo "${BLUE}Access: http://localhost:9000${NC}"
    echo "${BLUE}Access Key: $ACCESS_KEY${NC}"
    echo "${BLUE}Secret Key: $SECRET_KEY${NC}"

    # Save credentials
    echo "Access Key: $ACCESS_KEY" > .openendpoint-credentials
    echo "Secret Key: $SECRET_KEY" >> .openendpoint-credentials
    echo "${YELLOW}Credentials saved to .openendpoint-credentials${NC}"
}

# Binary installation
install_binary() {
    echo "${BLUE}Installing OpenEndpoint binary...${NC}"

    setup_directories

    # Download URL
    DOWNLOAD_URL="https://github.com/openendpoint/openendpoint/releases/download/v${VERSION}/openep-${PLATFORM}-${ARCH}.tar.gz"

    echo "${BLUE}Downloading from: $DOWNLOAD_URL${NC}"

    # Download and extract
    TEMP_DIR=$(mktemp -d)
    curl -fsSL "$DOWNLOAD_URL" | sudo tar xz -C "$TEMP_DIR"
    sudo mv "$TEMP_DIR/openep" "$INSTALL_DIR/bin/"
    sudo chmod +x "$INSTALL_DIR/bin/openep"

    # Create symlink
    sudo ln -sf "$INSTALL_DIR/bin/openep" /usr/local/bin/openep

    # Generate credentials
    ACCESS_KEY=$(generate_random | head -c 16)
    SECRET_KEY=$(generate_random | head -c 32)

    # Create config
    sudo tee "$CONFIG_DIR/config.yaml" > /dev/null << EOF
server:
  host: "0.0.0.0"
  port: 9000
  read_timeout: 30
  write_timeout: 30
  idle_timeout: 120

auth:
  access_key: "$ACCESS_KEY"
  secret_key: "$SECRET_KEY"

storage:
  data_dir: "$DATA_DIR"
  backend: flatfile

logging:
  level: "info"
  format: "json"
  output: "$LOG_DIR/app.log"

audit:
  enabled: true
  path: "$LOG_DIR/audit"
  max_size: 10485760
  max_backups: 10

rate_limit:
  enabled: true
  requests_per_second: 100
  burst: 1000
EOF

    sudo chown "$SERVICE_USER:$SERVICE_USER" "$CONFIG_DIR/config.yaml"
    sudo chmod 640 "$CONFIG_DIR/config.yaml"

    # Create systemd service
    sudo tee /etc/systemd/system/openendpoint.service > /dev/null << EOF
[Unit]
Description=OpenEndpoint Object Storage
Documentation=https://openendpoint.com/docs
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_USER
ExecStart=$INSTALL_DIR/bin/openep server --config $CONFIG_DIR/config.yaml
Restart=on-failure
RestartSec=5
StartLimitInterval=60s
StartLimitBurst=3

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$DATA_DIR $LOG_DIR

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=openendpoint

[Install]
WantedBy=multi-user.target
EOF

    # Reload and start service
    sudo systemctl daemon-reload
    sudo systemctl enable openendpoint
    sudo systemctl start openendpoint

    echo "${GREEN}✓ OpenEndpoint installed as systemd service${NC}"
    echo "${BLUE}Access: http://localhost:9000${NC}"
    echo "${BLUE}Access Key: $ACCESS_KEY${NC}"
    echo "${BLUE}Secret Key: $SECRET_KEY${NC}"

    # Save credentials
    echo "Access Key: $ACCESS_KEY" | sudo tee "$CONFIG_DIR/.credentials" > /dev/null
    echo "Secret Key: $SECRET_KEY" | sudo tee -a "$CONFIG_DIR/.credentials" > /dev/null
    sudo chmod 600 "$CONFIG_DIR/.credentials"
    echo "${YELLOW}Credentials saved to $CONFIG_DIR/.credentials${NC}"

    echo ""
    echo "${BLUE}Service commands:${NC}"
    echo "  sudo systemctl status openendpoint"
    echo "  sudo systemctl stop openendpoint"
    echo "  sudo systemctl start openendpoint"
    echo "  sudo journalctl -u openendpoint -f"
}

# Development installation (from source)
install_dev() {
    echo "${BLUE}Installing OpenEndpoint from source (development mode)...${NC}"

    if ! command_exists go; then
        echo "${RED}Go is not installed. Please install Go 1.22+ first.${NC}"
        exit 1
    fi

    # Check Go version
    GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
    echo "${BLUE}Go version: $GO_VERSION${NC}"

    # Clone repository
    if [ ! -d "openendpoint" ]; then
        git clone https://github.com/openendpoint/openendpoint.git
    fi
    cd openendpoint

    # Build
    go build -o bin/openep ./cmd/openep

    # Create config
    cat > config.yaml << EOF
server:
  host: "localhost"
  port: 9000

auth:
  access_key: "dev-access-key"
  secret_key: "dev-secret-key"

storage:
  data_dir: "./data"

logging:
  level: "debug"
  format: "console"
EOF

    echo "${GREEN}✓ OpenEndpoint built from source${NC}"
    echo "${BLUE}Run: ./bin/openep server --config config.yaml${NC}"
}

# Uninstall
uninstall() {
    echo "${YELLOW}Uninstalling OpenEndpoint...${NC}"

    # Stop and remove systemd service
    if [ -f /etc/systemd/system/openendpoint.service ]; then
        sudo systemctl stop openendpoint 2>/dev/null || true
        sudo systemctl disable openendpoint 2>/dev/null || true
        sudo rm -f /etc/systemd/system/openendpoint.service
        sudo systemctl daemon-reload
    fi

    # Remove Docker containers
    if command_exists docker-compose; then
        docker-compose down -v 2>/dev/null || true
    fi

    # Remove directories
    sudo rm -rf "$INSTALL_DIR" "$DATA_DIR" "$CONFIG_DIR" "$LOG_DIR"
    sudo rm -f /usr/local/bin/openep

    # Remove user
    if id "$SERVICE_USER" &>/dev/null; then
        sudo userdel "$SERVICE_USER" 2>/dev/null || true
    fi

    echo "${GREEN}✓ OpenEndpoint uninstalled${NC}"
}

# Main menu
show_menu() {
    echo ""
    echo "${BLUE}Select installation method:${NC}"
    echo "  1) Docker (Recommended - Easiest)"
    echo "  2) Binary (Systemd service)"
    echo "  3) Development (From source)"
    echo "  4) Uninstall"
    echo "  5) Exit"
    echo ""
}

# Main
main() {
    print_banner
    detect_os

    # Check command line argument
    case "${1:-}" in
        docker)
            install_docker
            exit 0
            ;;
        binary)
            install_binary
            exit 0
            ;;
        dev|source)
            install_dev
            exit 0
            ;;
        uninstall|remove)
            uninstall
            exit 0
            ;;
    esac

    # Interactive menu
    while true; do
        show_menu
        read -p "Enter choice [1-5]: " choice

        case $choice in
            1) install_docker; break;;
            2) install_binary; break;;
            3) install_dev; break;;
            4) uninstall; break;;
            5) echo "${BLUE}Goodbye!${NC}"; exit 0;;
            *) echo "${RED}Invalid choice${NC}";;
        esac
    done
}

# Run main function
main "$@"
