#!/bin/bash

# Mock S3 Server fixture for ctests
# Replaces SeaweedFS with in-process MockS3Server

set -e

MOCKS3_HOST=${MOCKS3_HOST:-127.0.0.1}
MOCKS3_PORT=${MOCKS3_PORT:-8080}
MOCKS3_PID=""

# Find available port if default is in use
find_available_port() {
    local port=$1
    while netstat -tuln | grep -q ":$port "; do
        port=$((port + 1))
    done
    echo $port
}

start_mocks3() {
    local build_dir_param="${1:-}"
    echo "Starting MockS3Server on ${MOCKS3_HOST}:${MOCKS3_PORT}"
    
    # Find an available port
    MOCKS3_PORT=$(find_available_port $MOCKS3_PORT)
    echo "Using port: $MOCKS3_PORT"
    
    # Use provided build directory or try to find it
    BUILD_DIR="$build_dir_param"
    if [ -z "$BUILD_DIR" ]; then
        # Find the build directory - look for fdbserver binary
        for dir in "../build_output" "../../build_output" "../../../build_output" "./build_output" "."; do
            if [ -f "$dir/bin/fdbserver" ]; then
                BUILD_DIR="$dir"
                break
            fi
        done
    fi

    if [ -z "$BUILD_DIR" ]; then
        echo "ERROR: Could not find fdbserver binary to run MockS3Server"
        return 1
    fi
    
    if [ ! -f "$BUILD_DIR/bin/fdbserver" ]; then
        echo "ERROR: fdbserver binary not found at $BUILD_DIR/bin/fdbserver"
        return 1
    fi

    # Start MockS3Server using fdbserver with the new role
    "$BUILD_DIR/bin/fdbserver" \
        --role mocks3server \
        --public-address "${MOCKS3_HOST}:${MOCKS3_PORT}" \
        --listen-address "${MOCKS3_HOST}:${MOCKS3_PORT}" &
    MOCKS3_PID=$!
    
    # Wait for server to be ready
    for i in {1..30}; do
        # Check if the process is still running
        if ! kill -0 $MOCKS3_PID 2>/dev/null; then
            echo "ERROR: MockS3Server process died"
            return 1
        fi
        
        # For now, just wait a bit since we're using a simulation
        if [ $i -gt 3 ]; then
            echo "MockS3Server ready"
            return 0
        fi
        sleep 1
    done
    
    echo "ERROR: MockS3Server failed to start"
    return 1
}

shutdown_mocks3() {
    if [ -n "$MOCKS3_PID" ]; then
        echo "Shutting down MockS3Server (PID: $MOCKS3_PID)"
        kill $MOCKS3_PID 2>/dev/null || true
        wait $MOCKS3_PID 2>/dev/null || true
    fi
    
}

get_mocks3_url() {
    echo "blobstore://mocks3:mocksecret@${MOCKS3_HOST}:${MOCKS3_PORT}"
}

# Provide compatibility functions with same names as seaweedfs_fixture.sh
run_mocks3() {
    start_mocks3 "$@"
}

get_weed_url() {
    get_mocks3_url
}

# Trap to ensure cleanup on script exit
trap shutdown_mocks3 EXIT

# Export functions for use by test scripts
export -f start_mocks3
export -f shutdown_mocks3  
export -f get_mocks3_url
export -f run_mocks3
export -f get_weed_url