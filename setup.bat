@echo off
REM OpenEndpoint One-Click Setup Script for Windows
REM Usage: setup.bat [docker|binary|dev|uninstall]

title OpenEndpoint Setup

echo.
echo ================================================
echo    OpenEndpoint Setup Script
echo    S3-Compatible Object Storage
echo ================================================
echo.

REM Check arguments
if "%~1"=="docker" goto :docker
if "%~1"=="binary" goto :binary
if "%~1"=="dev" goto :dev
if "%~1"=="uninstall" goto :uninstall
if "%~1"=="remove" goto :uninstall

REM Show menu
:menu
echo.
echo Select installation method:
echo   1) Docker (Recommended - Easiest)
echo   2) Binary (Windows Service)
echo   3) Development (From source)
echo   4) Uninstall
echo   5) Exit
echo.

set /p choice="Enter choice [1-5]: "

if "%choice%"=="1" goto :docker
if "%choice%"=="2" goto :binary
if "%choice%"=="3" goto :dev
if "%choice%"=="4" goto :uninstall
if "%choice%"=="5" goto :end

echo Invalid choice
goto :menu

:docker
echo.
echo [INFO] Installing with Docker...
echo.

REM Check if Docker is installed
docker --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker not found. Please install Docker Desktop first.
    echo [INFO] Download from: https://www.docker.com/products/docker-desktop
    pause
    exit /b 1
)

REM Create docker-compose.yml
echo Creating docker-compose.yml...
(
echo version: '3.8'
echo.
echo services:
echo   openendpoint:
echo     image: openendpoint/openendpoint:1.0.0
gecho     container_name: openendpoint
echo     ports:
echo       - "9000:9000"
echo     environment:
echo       - OPENEP_AUTH_ACCESS_KEY=minioadmin
echo       - OPENEP_AUTH_SECRET_KEY=minioadmin
echo       - OPENEP_SERVER_HOST=0.0.0.0
echo       - OPENEP_SERVER_PORT=9000
echo       - OPENEP_STORAGE_DATA_DIR=/data
echo     volumes:
echo       - openendpoint-data:/data
echo     restart: unless-stopped
echo.
echo volumes:
echo   openendpoint-data:
) > docker-compose.yml

REM Start services
echo Starting OpenEndpoint...
docker-compose up -d

if errorlevel 1 (
    echo [ERROR] Failed to start Docker containers
    pause
    exit /b 1
)

echo.
echo [SUCCESS] OpenEndpoint installed with Docker
echo [INFO] Access: http://localhost:9000
echo [INFO] Access Key: minioadmin
echo [INFO] Secret Key: minioadmin
echo.
pause
goto :end

:binary
echo.
echo [INFO] Installing binary version...
echo.

REM Check admin rights
net session >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Administrator privileges required.
    echo [INFO] Please run as Administrator.
    pause
    exit /b 1
)

set INSTALL_DIR=%ProgramFiles%\OpenEndpoint
set DATA_DIR=%ProgramData%\OpenEndpoint\Data
set CONFIG_DIR=%ProgramData%\OpenEndpoint\Config
set LOG_DIR=%ProgramData%\OpenEndpoint\Logs

REM Create directories
echo Creating directories...
if not exist "%INSTALL_DIR%" mkdir "%INSTALL_DIR%"
if not exist "%DATA_DIR%" mkdir "%DATA_DIR%"
if not exist "%CONFIG_DIR%" mkdir "%CONFIG_DIR%"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Download binary
echo Downloading OpenEndpoint...
powershell -Command "Invoke-WebRequest -Uri 'https://github.com/openendpoint/openendpoint/releases/download/v1.0.0/openep-windows-amd64.zip' -OutFile '%TEMP%\openep.zip'"

if errorlevel 1 (
    echo [ERROR] Failed to download binary
    pause
    exit /b 1
)

REM Extract
echo Extracting...
powershell -Command "Expand-Archive -Path '%TEMP%\openep.zip' -DestinationPath '%INSTALL_DIR%' -Force"

REM Create config
echo Creating configuration...
(
echo server:
echo   host: "0.0.0.0"
echo   port: 9000
echo   read_timeout: 30
echo   write_timeout: 30
echo   idle_timeout: 120
echo.
echo auth:
echo   access_key: "minioadmin"
echo   secret_key: "minioadmin"
echo.
echo storage:
echo   data_dir: "%DATA_DIR:\=/%"
echo   backend: flatfile
echo.
echo logging:
echo   level: "info"
echo   format: "json"
echo   output: "%LOG_DIR:\=/%/app.log"
) > "%CONFIG_DIR%\config.yaml"

REM Create startup script
echo Creating startup script...
(
echo @echo off
echo cd /d "%INSTALL_DIR%"
echo start "OpenEndpoint" "openep.exe" server --config "%CONFIG_DIR%\config.yaml"
) > "%INSTALL_DIR%\start.bat"

REM Create shortcut on Desktop
echo Creating desktop shortcut...
powershell -Command "$WshShell = New-Object -comObject WScript.Shell; $Shortcut = $WshShell.CreateShortcut('%PUBLIC%\Desktop\OpenEndpoint.lnk'); $Shortcut.TargetPath = '%INSTALL_DIR%\start.bat'; $Shortcut.WorkingDirectory = '%INSTALL_DIR%'; $Shortcut.IconLocation = '%SystemRoot%\System32\shell32.dll,3'; $Shortcut.Save()"

echo.
echo [SUCCESS] OpenEndpoint installed
echo [INFO] Installation directory: %INSTALL_DIR%
echo [INFO] Config directory: %CONFIG_DIR%
echo [INFO] Data directory: %DATA_DIR%
echo [INFO] Access: http://localhost:9000
echo [INFO] Access Key: minioadmin
echo [INFO] Secret Key: minioadmin
echo.
echo [INFO] Start OpenEndpoint using the desktop shortcut or: %INSTALL_DIR%\start.bat
echo.
pause
goto :end

:dev
echo.
echo [INFO] Installing from source (development mode)...
echo.

REM Check if Go is installed
go version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Go is not installed. Please install Go 1.22+ first.
    echo [INFO] Download from: https://go.dev/dl/
    pause
    exit /b 1
)

for /f "tokens=3" %%v in ('go version') do set GO_VERSION=%%v
echo [INFO] Go version: %GO_VERSION%

REM Clone repository
if not exist "openendpoint" (
    echo Cloning repository...
    git clone https://github.com/openendpoint/openendpoint.git
)

cd openendpoint

REM Build
echo Building...
go build -o bin\openep.exe .\cmd\openep

if errorlevel 1 (
    echo [ERROR] Build failed
    pause
    exit /b 1
)

REM Create config
echo Creating config...
(
echo server:
echo   host: "localhost"
echo   port: 9000
echo.
echo auth:
echo   access_key: "dev-access-key"
echo   secret_key: "dev-secret-key"
echo.
echo storage:
echo   data_dir: ".\data"
echo.
echo logging:
echo   level: "debug"
echo   format: "console"
) > config.yaml

echo.
echo [SUCCESS] OpenEndpoint built from source
echo [INFO] Run: .\bin\openep.exe server --config config.yaml
echo.
pause
goto :end

:uninstall
echo.
echo [INFO] Uninstalling OpenEndpoint...
echo.

REM Stop Docker containers
docker-compose down -v >nul 2>&1

REM Remove directories
rmdir /s /q "%ProgramFiles%\OpenEndpoint" 2>nul
rmdir /s /q "%ProgramData%\OpenEndpoint" 2>nul
rmdir /s /q "openendpoint" 2>nul

REM Remove desktop shortcut
del "%PUBLIC%\Desktop\OpenEndpoint.lnk" 2>nul

echo [SUCCESS] OpenEndpoint uninstalled
echo.
pause
goto :end

:end
echo.
echo [INFO] Setup completed
echo.
