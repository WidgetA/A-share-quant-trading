#!/bin/bash

# === MODULE PURPOSE ===
# Installation script for TongHuaShun (THS) iFinD SDK on Linux servers.
# This script handles SDK extraction, dependency checking, and Python module installation.

# === USAGE ===
# chmod +x scripts/install_ths_sdk.sh
# sudo ./scripts/install_ths_sdk.sh [OPTIONS]
#
# Options:
#   -f, --file <path>    Path to SDK tarball (default: auto-detect THSDataInterface_Linux_*.tar.gz)
#   -d, --dest <path>    Installation destination (default: /opt/ths_sdk)
#   -p, --python <path>  Python interpreter path (default: python3)
#   -h, --help           Show this help message

set -e

# === CONFIGURATION ===
SDK_TARBALL=""
INSTALL_DIR="/opt/ths_sdk"
PYTHON_BIN="python3"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# === COLOR OUTPUT ===
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# === ARGUMENT PARSING ===
while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--file)
            SDK_TARBALL="$2"
            shift 2
            ;;
        -d|--dest)
            INSTALL_DIR="$2"
            shift 2
            ;;
        -p|--python)
            PYTHON_BIN="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -f, --file <path>    Path to SDK tarball"
            echo "  -d, --dest <path>    Installation destination (default: /opt/ths_sdk)"
            echo "  -p, --python <path>  Python interpreter path (default: python3)"
            echo "  -h, --help           Show this help message"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# === FUNCTIONS ===

check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run as root (use sudo)"
        exit 1
    fi
}

detect_arch() {
    # Detect system architecture: 32-bit or 64-bit
    ARCH=$(uname -m)
    if [[ "$ARCH" == "x86_64" ]]; then
        SDK_ARCH="64"
        SDK_BIN_DIR="bin64"
        log_info "Detected 64-bit system"
    else
        SDK_ARCH="32"
        SDK_BIN_DIR="bin"
        log_info "Detected 32-bit system"
    fi
}

detect_package_manager() {
    # Detect package manager: apt-get (Debian/Ubuntu) or yum (CentOS/RHEL)
    if command -v apt-get &> /dev/null; then
        PKG_MANAGER="apt-get"
        PKG_INSTALL="apt-get install -y"
        log_info "Detected package manager: apt-get"
    elif command -v yum &> /dev/null; then
        PKG_MANAGER="yum"
        PKG_INSTALL="yum install -y"
        log_info "Detected package manager: yum"
    elif command -v dnf &> /dev/null; then
        PKG_MANAGER="dnf"
        PKG_INSTALL="dnf install -y"
        log_info "Detected package manager: dnf"
    else
        log_error "No supported package manager found (apt-get/yum/dnf)"
        exit 1
    fi
}

find_sdk_tarball() {
    # Auto-detect SDK tarball if not specified
    # Search order: vendor/ directory first, then project root
    if [[ -z "$SDK_TARBALL" ]]; then
        # First check vendor/ directory
        SDK_TARBALL=$(find "$PROJECT_ROOT/vendor" -maxdepth 1 -name "THSDataInterface_Linux_*.tar.gz" 2>/dev/null | head -n 1)
        # Fallback to project root
        if [[ -z "$SDK_TARBALL" ]]; then
            SDK_TARBALL=$(find "$PROJECT_ROOT" -maxdepth 1 -name "THSDataInterface_Linux_*.tar.gz" | head -n 1)
        fi
        if [[ -z "$SDK_TARBALL" ]]; then
            log_error "SDK tarball not found in vendor/ or project root"
            log_error "Please place THSDataInterface_Linux_*.tar.gz in vendor/ directory"
            log_error "Or specify path with -f option"
            exit 1
        fi
    fi

    if [[ ! -f "$SDK_TARBALL" ]]; then
        log_error "SDK tarball not found: $SDK_TARBALL"
        exit 1
    fi

    log_info "Using SDK tarball: $SDK_TARBALL"
}

extract_sdk() {
    log_info "Extracting SDK to $INSTALL_DIR ..."

    # Create installation directory
    mkdir -p "$INSTALL_DIR"

    # Extract tarball
    tar -xzf "$SDK_TARBALL" -C "$INSTALL_DIR"

    log_info "SDK extracted successfully"
}

check_dependencies() {
    # Check library dependencies using ldd
    log_info "Checking library dependencies..."

    local libs=(
        "$INSTALL_DIR/$SDK_BIN_DIR/libShellExport.so"
        "$INSTALL_DIR/$SDK_BIN_DIR/libFTDataInterface.so"
        "$INSTALL_DIR/$SDK_BIN_DIR/hqdatafeed"
    )

    local missing_deps=()

    for lib in "${libs[@]}"; do
        if [[ -f "$lib" ]]; then
            log_info "Checking: $(basename "$lib")"
            # Find missing dependencies (lines containing "not found")
            local not_found=$(ldd "$lib" 2>&1 | grep "not found" || true)
            if [[ -n "$not_found" ]]; then
                log_warn "Missing dependencies for $(basename "$lib"):"
                echo "$not_found"
                # Extract library names
                while IFS= read -r line; do
                    local dep=$(echo "$line" | awk '{print $1}')
                    missing_deps+=("$dep")
                done <<< "$not_found"
            else
                log_info "  All dependencies satisfied"
            fi
        else
            log_warn "Library not found: $lib"
        fi
    done

    # Return missing dependencies
    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        echo "${missing_deps[@]}"
        return 1
    fi
    return 0
}

install_common_dependencies() {
    # Install common dependencies that THS SDK typically needs
    log_info "Installing common dependencies..."

    if [[ "$PKG_MANAGER" == "apt-get" ]]; then
        apt-get update
        $PKG_INSTALL \
            libstdc++6 \
            libgcc1 \
            libc6 \
            libssl-dev \
            libcurl4-openssl-dev \
            zlib1g \
            libpthread-stubs0-dev
    else
        # yum/dnf
        $PKG_INSTALL \
            libstdc++ \
            glibc \
            openssl-devel \
            libcurl-devel \
            zlib
    fi

    log_info "Common dependencies installed"
}

setup_library_path() {
    # Add SDK library path to system
    log_info "Setting up library path..."

    local ld_conf="/etc/ld.so.conf.d/ths_sdk.conf"
    echo "$INSTALL_DIR/$SDK_BIN_DIR" > "$ld_conf"
    ldconfig

    log_info "Library path configured: $INSTALL_DIR/$SDK_BIN_DIR"
}

install_python_module() {
    # Run the official iFinDPy installer
    log_info "Installing Python module iFinDPy..."

    local installer="$INSTALL_DIR/$SDK_BIN_DIR/installiFinDPy.py"

    if [[ ! -f "$installer" ]]; then
        log_error "Python installer not found: $installer"
        exit 1
    fi

    # Run installer with install directory as argument
    $PYTHON_BIN "$installer" "$INSTALL_DIR"

    log_info "Python module installed successfully"
}

verify_installation() {
    # Verify the installation by importing the module
    log_info "Verifying installation..."

    if $PYTHON_BIN -c "from iFinDPy import *; print('iFinDPy imported successfully')" 2>/dev/null; then
        log_info "Verification passed: iFinDPy module is working"
    else
        log_warn "Verification failed: Could not import iFinDPy"
        log_warn "You may need to add the SDK path to PYTHONPATH manually"
        log_warn "  export PYTHONPATH=\$PYTHONPATH:$INSTALL_DIR/$SDK_BIN_DIR"
    fi
}

print_summary() {
    echo ""
    echo "========================================"
    log_info "THS SDK Installation Complete!"
    echo "========================================"
    echo ""
    echo "Installation directory: $INSTALL_DIR"
    echo "SDK architecture: ${SDK_ARCH}-bit"
    echo "Binary directory: $INSTALL_DIR/$SDK_BIN_DIR"
    echo ""
    echo "Usage:"
    echo "  from iFinDPy import *"
    echo ""
    echo "Sample code: $INSTALL_DIR/$SDK_BIN_DIR/sample.py"
    echo ""
    echo "If import fails, add to your environment:"
    echo "  export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:$INSTALL_DIR/$SDK_BIN_DIR"
    echo "  export PYTHONPATH=\$PYTHONPATH:$INSTALL_DIR/$SDK_BIN_DIR"
    echo ""
}

# === MAIN ===
main() {
    echo "========================================"
    echo "  THS iFinD SDK Installer for Linux"
    echo "========================================"
    echo ""

    check_root
    detect_arch
    detect_package_manager
    find_sdk_tarball

    # Extract SDK
    extract_sdk

    # Install common dependencies first
    install_common_dependencies

    # Check and report missing dependencies
    if ! check_dependencies; then
        log_warn "Some dependencies are missing. Please install them manually."
        log_warn "After installing, run: sudo ldconfig"
    fi

    # Setup library path
    setup_library_path

    # Install Python module
    install_python_module

    # Verify
    verify_installation

    # Print summary
    print_summary
}

main "$@"
