#!/bin/bash
set -e

# ═══════════════════════════════════════════════════════════════════════
# OpenEndpoint Release Script
# ═══════════════════════════════════════════════════════════════════════

VERSION=${1:-$(git describe --tags --always --dirty 2>/dev/null || echo "v0.1.0")}
BUILD_TIME=$(date -u '+%Y-%m-%d_%H:%M:%S')
BINARY_NAME="openep"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}          OpenEndpoint Release Build System                ${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${GREEN}Version: ${VERSION}${NC}"
echo -e "${GREEN}Build Time: ${BUILD_TIME}${NC}"
echo ""

# Create release directory
echo -e "${YELLOW}Creating release directory...${NC}"
mkdir -p release
rm -rf release/*

# Build for different platforms
PLATFORMS=(
    "linux/amd64"
    "linux/arm64"
    "darwin/amd64"
    "darwin/arm64"
    "windows/amd64"
)

for PLATFORM in "${PLATFORMS[@]}"; do
    IFS='/' read -r GOOS GOARCH <<< "$PLATFORM"
    OUTPUT="${BINARY_NAME}-${GOOS}-${GOARCH}"

    if [ "$GOOS" = "windows" ]; then
        OUTPUT="${OUTPUT}.exe"
    fi

    echo -e "${YELLOW}Building for ${GOOS}/${GOARCH}...${NC}"

    GOOS=$GOOS GOARCH=$GOARCH CGO_ENABLED=0 go build \
        -ldflags "-X main.version=${VERSION} -X main.buildTime=${BUILD_TIME} -s -w" \
        -o "release/${OUTPUT}" \
        ./cmd/openep

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Built release/${OUTPUT}${NC}"
    else
        echo -e "${RED}✗ Failed to build ${OUTPUT}${NC}"
        exit 1
    fi
done

# Create archives
echo ""
echo -e "${YELLOW}Creating archives...${NC}"
cd release

for BINARY in ${BINARY_NAME}-*-amd64 ${BINARY_NAME}-*-arm64; do
    if [[ "$BINARY" == *.exe ]]; then
        # Windows - use zip
        ZIP_NAME="${BINARY%.exe}.zip"
        zip "$ZIP_NAME" "$BINARY"
        rm "$BINARY"
        echo -e "${GREEN}✓ Created ${ZIP_NAME}${NC}"
    else
        # Unix - use tar.gz
        TAR_NAME="${BINARY}.tar.gz"
        tar -czvf "$TAR_NAME" "$BINARY"
        rm "$BINARY"
        echo -e "${GREEN}✓ Created ${TAR_NAME}${NC}"
    fi
done

# Create checksums
echo ""
echo -e "${YELLOW}Creating checksums...${NC}"
sha256sum *.tar.gz *.zip > SHA256SUMS
echo -e "${GREEN}✓ Created SHA256SUMS${NC}"

cd ..

# Build Docker image
echo ""
echo -e "${YELLOW}Building Docker image...${NC}"
if command -v docker &> /dev/null; then
    docker build \
        -t openendpoint/${BINARY_NAME}:${VERSION} \
        -t openendpoint/${BINARY_NAME}:latest \
        -f deploy/docker/Dockerfile \
        .
    echo -e "${GREEN}✓ Docker image built${NC}"
else
    echo -e "${YELLOW}Docker not found, skipping image build${NC}"
fi

# Summary
echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}                 Release Complete!                         ${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo ""
echo "Artifacts:"
ls -la release/
echo ""
echo "Next steps:"
echo "  1. Test the binaries"
echo "  2. Push Docker image: docker push openendpoint/${BINARY_NAME}:${VERSION}"
echo "  3. Create GitHub release"
echo "  4. Update documentation"
echo ""
