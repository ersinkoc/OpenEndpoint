#
# OpenEndpoint One-Click Setup Script for Windows
# Supports: Windows 10/11, Windows Server 2019/2022
# Usage: .\setup.ps1 [docker|binary|dev|uninstall]
#

param(
    [Parameter(Position=0)]
    [ValidateSet("docker", "binary", "dev", "uninstall", "")]
    [string]$Action = ""
)

# Configuration
$VERSION = "1.0.0"
$INSTALL_DIR = "$env:ProgramFiles\OpenEndpoint"
$DATA_DIR = "$env:ProgramData\OpenEndpoint\Data"
$CONFIG_DIR = "$env:ProgramData\OpenEndpoint\Config"
$LOG_DIR = "$env:ProgramData\OpenEndpoint\Logs"
$SERVICE_NAME = "OpenEndpoint"

# Colors
function Write-Color($Text, $Color) {
    Write-Host $Text -ForegroundColor $Color
}

function Print-Banner {
    Write-Color @"
╔════════════════════════════════════════════════════════════╗
║                                                            ║
║              OpenEndpoint Setup Script                     ║
║              S3-Compatible Object Storage                  ║
║                                                            ║
╚════════════════════════════════════════════════════════════╝
""" Cyan
}

function Test-Command($Command) {
    return [bool](Get-Command -Name $Command -ErrorAction SilentlyContinue)
}

function Generate-RandomString($Length = 32) {
    $chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    $result = -join ((1..$Length) | ForEach-Object { $chars[(Get-Random -Maximum $chars.Length)] })
    return $result
}

function Test-Admin {
    return ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")
}

function Install-Docker {
    Write-Color "Installing OpenEndpoint with Docker..." Cyan

    if (-not (Test-Command "docker")) {
        Write-Color "Docker not found. Please install Docker Desktop first." Red
        Write-Color "Download from: https://www.docker.com/products/docker-desktop" Yellow
        exit 1
    }

    # Create docker-compose.yml
    $composeContent = @"
version: '3.8'

services:
  openendpoint:
    image: openendpoint/openendpoint:1.0.0
    container_name: openendpoint
    ports:
      - "9000:9000"
    environment:
      - OPENEP_AUTH_ACCESS_KEY=${env:ACCESS_KEY}
      - OPENEP_AUTH_SECRET_KEY=${env:SECRET_KEY}
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
"@

    $composeContent | Out-File -FilePath "docker-compose.yml" -Encoding UTF8

    # Generate credentials
    if (-not $env:ACCESS_KEY) {
        $env:ACCESS_KEY = Generate-RandomString 16
        $env:SECRET_KEY = Generate-RandomString 32
    }

    # Start services
    docker-compose up -d

    Write-Color "✓ OpenEndpoint installed with Docker" Green
    Write-Color "Access: http://localhost:9000" Cyan
    Write-Color "Access Key: $($env:ACCESS_KEY)" Cyan
    Write-Color "Secret Key: $($env:SECRET_KEY)" Cyan

    # Save credentials
    "Access Key: $($env:ACCESS_KEY)" | Out-File -FilePath ".openendpoint-credentials" -Encoding UTF8
    "Secret Key: $($env:SECRET_KEY)" | Out-File -FilePath ".openendpoint-credentials" -Append -Encoding UTF8
    Write-Color "Credentials saved to .openendpoint-credentials" Yellow
}

function Install-Binary {
    Write-Color "Installing OpenEndpoint binary..." Cyan

    if (-not (Test-Admin)) {
        Write-Color "Administrator privileges required. Please run as Administrator." Red
        exit 1
    }

    # Create directories
    New-Item -ItemType Directory -Force -Path $INSTALL_DIR | Out-Null
    New-Item -ItemType Directory -Force -Path $DATA_DIR | Out-Null
    New-Item -ItemType Directory -Force -Path $CONFIG_DIR | Out-Null
    New-Item -ItemType Directory -Force -Path $LOG_DIR | Out-Null

    # Detect architecture
    $arch = if ([Environment]::Is64BitOperatingSystem) { "amd64" } else { "386" }

    # Download URL
    $downloadUrl = "https://github.com/openendpoint/openendpoint/releases/download/v$VERSION/openep-windows-$arch.zip"

    Write-Color "Downloading from: $downloadUrl" Cyan

    # Download and extract
    $tempDir = [System.IO.Path]::GetTempPath() + [System.Guid]::NewGuid().ToString()
    New-Item -ItemType Directory -Force -Path $tempDir | Out-Null

    $zipFile = "$tempDir\openep.zip"
    Invoke-WebRequest -Uri $downloadUrl -OutFile $zipFile

    Expand-Archive -Path $zipFile -DestinationPath $tempDir -Force
    Move-Item -Path "$tempDir\openep.exe" -Destination "$INSTALL_DIR\openep.exe" -Force

    # Add to PATH
    $currentPath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    if ($currentPath -notlike "*$INSTALL_DIR*") {
        [Environment]::SetEnvironmentVariable("Path", "$currentPath;$INSTALL_DIR", "Machine")
        Write-Color "Added to PATH. Please restart your terminal." Yellow
    }

    # Generate credentials
    $accessKey = Generate-RandomString 16
    $secretKey = Generate-RandomString 32

    # Create config
    $configContent = @"
server:
  host: "0.0.0.0"
  port: 9000
  read_timeout: 30
  write_timeout: 30
  idle_timeout: 120

auth:
  access_key: "$accessKey"
  secret_key: "$secretKey"

storage:
  data_dir: "$DATA_DIR"
  backend: flatfile

logging:
  level: "info"
  format: "json"
  output: "$LOG_DIR\app.log"

audit:
  enabled: true
  path: "$LOG_DIR\audit"
  max_size: 10485760
  max_backups: 10

rate_limit:
  enabled: true
  requests_per_second: 100
  burst: 1000
"@

    $configContent | Out-File -FilePath "$CONFIG_DIR\config.yaml" -Encoding UTF8

    # Create Windows Service
    $nssmPath = "$INSTALL_DIR\nssm.exe"
    if (-not (Test-Path $nssmPath)) {
        # Download NSSM for service management
        Invoke-WebRequest -Uri "https://nssm.cc/release/nssm-2.24.zip" -OutFile "$tempDir\nssm.zip"
        Expand-Archive -Path "$tempDir\nssm.zip" -DestinationPath $tempDir -Force
        Copy-Item -Path "$tempDir\nssm-2.24\win64\nssm.exe" -Destination $nssmPath -Force
    }

    # Install service
    & $nssmPath install $SERVICE_NAME "$INSTALL_DIR\openep.exe"
    & $nssmPath set $SERVICE_NAME AppParameters "server --config `"$CONFIG_DIR\config.yaml`""
    & $nssmPath set $SERVICE_NAME AppDirectory $INSTALL_DIR
    & $nssmPath set $SERVICE_NAME DisplayName "OpenEndpoint Object Storage"
    & $nssmPath set $SERVICE_NAME Description "S3-Compatible Object Storage Server"
    & $nssmPath set $SERVICE_NAME Start SERVICE_AUTO_START

    # Start service
    Start-Service -Name $SERVICE_NAME

    # Cleanup
    Remove-Item -Path $tempDir -Recurse -Force

    Write-Color "✓ OpenEndpoint installed as Windows Service" Green
    Write-Color "Access: http://localhost:9000" Cyan
    Write-Color "Access Key: $accessKey" Cyan
    Write-Color "Secret Key: $secretKey" Cyan

    # Save credentials
    "Access Key: $accessKey" | Out-File -FilePath "$CONFIG_DIR\.credentials" -Encoding UTF8
    "Secret Key: $secretKey" | Out-File -FilePath "$CONFIG_DIR\.credentials" -Append -Encoding UTF8

    Write-Color "" Cyan
    Write-Color "Service commands:" Cyan
    Write-Color "  Get-Service $SERVICE_NAME" Cyan
    Write-Color "  Start-Service $SERVICE_NAME" Cyan
    Write-Color "  Stop-Service $SERVICE_NAME" Cyan
    Write-Color "  & '$nssmPath' edit $SERVICE_NAME" Cyan
}

function Install-Dev {
    Write-Color "Installing OpenEndpoint from source (development mode)..." Cyan

    if (-not (Test-Command "go")) {
        Write-Color "Go is not installed. Please install Go 1.22+ first." Red
        Write-Color "Download from: https://go.dev/dl/" Yellow
        exit 1
    }

    $goVersion = (go version) -replace 'go version go', '' -replace ' .*', ''
    Write-Color "Go version: $goVersion" Cyan

    # Clone repository
    if (-not (Test-Path "openendpoint")) {
        git clone https://github.com/openendpoint/openendpoint.git
    }
    Set-Location openendpoint

    # Build
    go build -o bin\openep.exe .\cmd\openep

    # Create config
    $configContent = @"
server:
  host: "localhost"
  port: 9000

auth:
  access_key: "dev-access-key"
  secret_key: "dev-secret-key"

storage:
  data_dir: ".\data"

logging:
  level: "debug"
  format: "console"
"@

    $configContent | Out-File -FilePath "config.yaml" -Encoding UTF8

    Write-Color "✓ OpenEndpoint built from source" Green
    Write-Color "Run: .\bin\openep.exe server --config config.yaml" Cyan
}

function Uninstall-OpenEndpoint {
    Write-Color "Uninstalling OpenEndpoint..." Yellow

    # Stop and remove service
    if (Get-Service -Name $SERVICE_NAME -ErrorAction SilentlyContinue) {
        Stop-Service -Name $SERVICE_NAME -Force -ErrorAction SilentlyContinue
        & "$INSTALL_DIR\nssm.exe" remove $SERVICE_NAME confirm
    }

    # Stop Docker containers
    if (Test-Command "docker-compose") {
        docker-compose down -v 2>$null
    }

    # Remove directories
    Remove-Item -Path $INSTALL_DIR -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -Path $DATA_DIR -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -Path $CONFIG_DIR -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -Path $LOG_DIR -Recurse -Force -ErrorAction SilentlyContinue

    Write-Color "✓ OpenEndpoint uninstalled" Green
}

function Show-Menu {
    Write-Host ""
    Write-Color "Select installation method:" Cyan
    Write-Host "  1) Docker (Recommended - Easiest)"
    Write-Host "  2) Binary (Windows Service)"
    Write-Host "  3) Development (From source)"
    Write-Host "  4) Uninstall"
    Write-Host "  5) Exit"
    Write-Host ""
}

# Main
Print-Banner

switch ($Action.ToLower()) {
    "docker" { Install-Docker; exit }
    "binary" { Install-Binary; exit }
    "dev" { Install-Dev; exit }
    "uninstall" { Uninstall-OpenEndpoint; exit }
}

# Interactive menu
while ($true) {
    Show-Menu
    $choice = Read-Host "Enter choice [1-5]"

    switch ($choice) {
        "1" { Install-Docker; break }
        "2" { Install-Binary; break }
        "3" { Install-Dev; break }
        "4" { Uninstall-OpenEndpoint; break }
        "5" { Write-Color "Goodbye!" Cyan; exit }
        default { Write-Color "Invalid choice" Red }
    }
}
