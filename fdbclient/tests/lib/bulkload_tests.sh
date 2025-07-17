#!/bin/bash
# Bulkload test module for FDB bash tests
# Tests bulk dump and bulk load functionality using S3 or SeaweedFS

# Guard against multiple sourcing
if [[ "${BULKLOAD_TESTS_LOADED:-false}" == "true" ]]; then
    return 0
fi

# Load test runner if not already loaded
if [[ "${TEST_RUNNER_LOADED:-false}" != "true" ]]; then
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    source "${script_dir}/test_runner.sh"
fi

# Bulkload test configuration
declare BULKLOAD_TESTS_LOADED=true
declare BULKLOAD_CONFIG_LOADED=false
declare BULKLOAD_HOST=""
declare BULKLOAD_BUCKET=""
declare BULKLOAD_REGION=""
declare BULKLOAD_CREDENTIALS_FILE=""
declare BULKLOAD_QUERY_STR=""
declare BULKLOAD_PATH_PREFIX=""
declare BULKLOAD_USE_S3=""
declare BULKLOAD_TLS_CA_FILE=""
declare BULKLOAD_HTTP_VERBOSE_LEVEL=""

# Load bulkload configuration
function setup_bulkload_tests() {
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    
    # Parse command line arguments - use the arguments passed to this function
    parse_args "$@"
    
    # Load configuration module
    source "${script_dir}/s3client_config.sh"
    
    # Initialize configuration
    init_config "${PARSED_BUILD_DIR}" "${PARSED_SCRATCH_DIR}"
    
    # Setup S3 or SeaweedFS
    if [[ "${USE_S3}" == "true" ]]; then
        setup_s3_config "${PARSED_BUILD_DIR}" "${PARSED_SCRATCH_DIR}"
    else
        setup_seaweedfs_config "${PARSED_BUILD_DIR}" "${PARSED_SCRATCH_DIR}"
    fi
    
    # Export configuration for tests
    BULKLOAD_HOST="${host}"
    BULKLOAD_BUCKET="${bucket}"
    BULKLOAD_REGION="${region}"
    BULKLOAD_CREDENTIALS_FILE="${blob_credentials_file}"
    BULKLOAD_QUERY_STR="${query_str}"
    BULKLOAD_PATH_PREFIX="${path_prefix}"
    BULKLOAD_USE_S3="${USE_S3}"
    BULKLOAD_TLS_CA_FILE="${TLS_CA_FILE}"
    BULKLOAD_HTTP_VERBOSE_LEVEL="${HTTP_VERBOSE_LEVEL}"
    
    # Setup FDB cluster
    setup_fdb_cluster "${PARSED_BUILD_DIR}" "${PARSED_SCRATCH_DIR}"
    
    BULKLOAD_CONFIG_LOADED=true
    
    log "Bulkload configuration loaded"
    log "Host: ${BULKLOAD_HOST}"
    log "Bucket: ${BULKLOAD_BUCKET}"
    log "Region: ${BULKLOAD_REGION}"
    log "Use S3: ${BULKLOAD_USE_S3}"
}

# Setup FDB cluster for bulkload tests
function setup_fdb_cluster() {
    local build_dir="$1"
    local scratch_dir="$2"
    
    # Source FDB cluster fixture
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    
    if ! source "${script_dir}/fdb_cluster_fixture.sh"; then
        err "Failed to source fdb_cluster_fixture.sh"
        return 1
    fi
    
    # Start FDB cluster with 9 SSs (bulk load tries to avoid loading back on to the team it dumped from)
    local knobs=("--knob_http_verbose_level=${HTTP_VERBOSE_LEVEL}")
    if ! start_fdb_cluster "${script_dir}/../.." "${build_dir}" "${TEST_SCRATCH_DIR}" 9 "${knobs[@]}"; then
        err "Failed start FDB cluster"
        return 1
    fi
    
    log "FDB cluster is up"
    
    # Register cleanup
    register_cleanup cleanup_fdb_cluster
}

# Cleanup FDB cluster
function cleanup_fdb_cluster() {
    if type shutdown_fdb_cluster &> /dev/null; then
        shutdown_fdb_cluster
    fi
}

# Build a test URL
# $1: test name
function build_bulkload_url() {
    local test_name="$1"
    echo "blobstore://${BULKLOAD_HOST}/${BULKLOAD_PATH_PREFIX}/${test_name}?${BULKLOAD_QUERY_STR}"
}

# Run bulkdump command
# $1: url to dump to
# $2: scratch directory  
# $3: credentials file
# $4: build directory
function bulkdump() {
    local url="$1"
    local scratch_dir="$2"
    local credentials="$3"
    local build_dir="$4"
    
    # Enable bulkdump mode
    if ! "${build_dir}/bin/fdbcli" \
        -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
        --exec "bulkdump mode on"; then
        err "Bulkdump mode on failed"
        return 1
    fi
    
    # Start bulkdump
    if ! "${build_dir}/bin/fdbcli" \
        -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
        --exec "bulkdump dump \"\" \xff \"${url}\"" > /dev/null; then
        err "Bulkdump start failed"
        return 1
    fi
    
    local output=""
    local jobid=""
    
    # Wait for job to start and get job ID
    while true; do
        if ! output=$("${build_dir}/bin/fdbcli" \
            -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
            --exec "bulkdump status"); then
            err "Bulkdump status failed"
            return 1
        fi
        
        if ! echo "${output}" | grep "Running bulk dumping job:" > /dev/null; then
            break
        elif [[ -z "${jobid}" ]]; then
            if line=$(echo "${output}" | grep "Running bulk dumping job:"); then
                jobid=$(echo "${line}" | sed -e 's/.*Running bulk dumping job://' | xargs)
            fi
        fi
        sleep 5
    done
    
    # Wait for job to complete
    while true; do
        if ! output=$("${build_dir}/bin/fdbcli" \
            -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
            --exec "bulkdump status"); then
            err "Bulkdump status failed"
            return 1
        fi
        
        if echo "${output}" | grep "No bulk dumping job is running" &> /dev/null; then
            break
        fi
        sleep 5
    done
    
    echo "${jobid}"
}

# Run bulkload command
# $1: url to load from
# $2: scratch directory
# $3: credentials file
# $4: build directory
# $5: job ID
function bulkload() {
    local url="$1"
    local scratch_dir="$2"
    local credentials="$3"
    local build_dir="$4"
    local jobid="$5"
    
    # Start bulkload
    if ! "${build_dir}/bin/fdbcli" \
        -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
        --exec "bulkload start \"${url}\" \"${jobid}\"" > /dev/null; then
        err "Bulkload start failed"
        return 1
    fi
    
    local output=""
    
    # Wait for bulkload to complete
    while true; do
        if ! output=$("${build_dir}/bin/fdbcli" \
            -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
            --exec "bulkload status"); then
            err "Bulkload status failed"
            return 1
        fi
        
        if echo "${output}" | grep "No bulk loading job is running" &> /dev/null; then
            break
        fi
        sleep 5
    done
}

# Test basic bulkdump and bulkload functionality
function test_bulkload_basic() {
    local test_name="basic_bulkdump_and_bulkload"
    local url
    url=$(build_bulkload_url "${test_name}")
    
    log "Loading initial data into FDB"
    if ! load_data "${TEST_BUILD_DIR}" "${TEST_SCRATCH_DIR}"; then
        err "Failed loading data into FDB"
        return 1
    fi
    
    # Clear S3 location if using real S3
    if [[ "${BULKLOAD_USE_S3}" == "true" ]]; then
        if ! "${TEST_BUILD_DIR}/bin/s3client" \
            --knob_http_verbose_level="${BULKLOAD_HTTP_VERBOSE_LEVEL}" \
            --tls-ca-file "${BULKLOAD_TLS_CA_FILE}" \
            --blob-credentials "${BULKLOAD_CREDENTIALS_FILE}" \
            --log --logdir "${TEST_SCRATCH_DIR}/logs" \
            rm "${url}"; then
            err "Failed rm of ${url}"
            return 1
        fi
    fi
    
    log "Running bulkdump to ${url}"
    local jobid
    if ! jobid=$(bulkdump "${url}" "${TEST_SCRATCH_DIR}" "${BULKLOAD_CREDENTIALS_FILE}" "${TEST_BUILD_DIR}"); then
        err "Failed bulkdump"
        return 1
    fi
    
    log "Clearing data from FDB"
    if ! clear_data "${TEST_BUILD_DIR}" "${TEST_SCRATCH_DIR}"; then
        err "Failed clear data in FDB"
        return 1
    fi
    
    log "Running bulkload from ${url}"
    if ! bulkload "${url}" "${TEST_SCRATCH_DIR}" "${BULKLOAD_CREDENTIALS_FILE}" "${TEST_BUILD_DIR}" "${jobid}"; then
        err "Failed bulkload"
        return 1
    fi
    
    log "Verifying restored data"
    if ! verify_data "${TEST_BUILD_DIR}" "${TEST_SCRATCH_DIR}"; then
        err "Failed verification of data in FDB"
        return 1
    fi
    
    log "Checking for Severity=40 errors"
    if ! grep_for_severity40 "${TEST_SCRATCH_DIR}"; then
        err "Found Severity=40 errors in logs"
        return 1
    fi
}

# Run bulkload test suite
function run_bulkload_test_suite() {
    run_test_suite "Bulkload" setup_bulkload_tests "$@" \
        "test_bulkload_basic"
}

# Parse command line arguments (reuse s3client argument parsing)
function parse_args() {
    PARSED_BUILD_DIR="$1"
    PARSED_SCRATCH_DIR="${2:-${TMPDIR:-/tmp}}"
    shift 2
    
    # Parse remaining arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --use-s3)
                USE_S3="true"
                shift
                ;;
            --host)
                host="$2"
                shift 2
                ;;
            --bucket)
                bucket="$2"
                shift 2
                ;;
            --region)
                region="$2"
                shift 2
                ;;
            --blob-credentials-file)
                blob_credentials_file="$2"
                shift 2
                ;;
            *)
                err "Unknown argument: $1"
                return 1
                ;;
        esac
    done
} 