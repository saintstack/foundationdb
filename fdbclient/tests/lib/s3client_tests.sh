#!/bin/bash
# S3Client test module
# Contains all S3Client test functions in a modular, reusable format

# Guard against multiple sourcing
if [[ "${S3CLIENT_TESTS_LOADED:-false}" == "true" ]]; then
    return 0
fi

# Get script directory
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Ensure test runner is loaded (but don't double-source)
if [[ "${TEST_RUNNER_LOADED:-false}" != "true" ]]; then
    source "${script_dir}/lib/test_runner.sh"
fi

# S3Client test configuration
declare S3CLIENT_TESTS_LOADED=true
declare S3CLIENT_CONFIG_LOADED=false
declare S3CLIENT_HOST=""
declare S3CLIENT_BUCKET=""
declare S3CLIENT_REGION=""
declare S3CLIENT_CREDENTIALS_FILE=""
declare S3CLIENT_QUERY_STR=""
declare S3CLIENT_PATH_PREFIX=""
declare S3CLIENT_USE_S3=""
declare S3CLIENT_TLS_CA_FILE=""
declare S3CLIENT_HTTP_VERBOSE_LEVEL=""

# Load S3Client configuration
function load_s3client_config() {
    if [[ "${S3CLIENT_CONFIG_LOADED}" == "true" ]]; then
        return 0
    fi
    
    # Load configuration module
    source "${script_dir}/lib/s3client_config.sh"
    
    # Parse command line arguments - use the arguments passed to this function
    parse_args "$@"
    
    # Initialize configuration
    init_config "${PARSED_BUILD_DIR}" "${PARSED_SCRATCH_DIR}"
    
    # Setup S3 or SeaweedFS
    if [[ "${USE_S3}" == "true" ]]; then
        setup_s3_config "${PARSED_BUILD_DIR}" "${PARSED_SCRATCH_DIR}"
    else
        setup_seaweedfs_config "${PARSED_BUILD_DIR}" "${PARSED_SCRATCH_DIR}"
    fi
    
    # Apply command line overrides
    apply_arg_overrides
    
    # Export configuration for tests
    S3CLIENT_HOST="${host}"
    S3CLIENT_BUCKET="${bucket}"
    S3CLIENT_REGION="${region}"
    S3CLIENT_CREDENTIALS_FILE="${blob_credentials_file}"
    S3CLIENT_QUERY_STR="${query_str}"
    S3CLIENT_PATH_PREFIX="${path_prefix}"
    S3CLIENT_USE_S3="${USE_S3}"
    S3CLIENT_TLS_CA_FILE="${TLS_CA_FILE}"
    S3CLIENT_HTTP_VERBOSE_LEVEL="${HTTP_VERBOSE_LEVEL}"
    
    S3CLIENT_CONFIG_LOADED=true
    
    log "S3Client configuration loaded"
    log "Host: ${S3CLIENT_HOST}"
    log "Bucket: ${S3CLIENT_BUCKET}"
    log "Region: ${S3CLIENT_REGION}"
    log "Use S3: ${S3CLIENT_USE_S3}"
}

# Build a test URL
# $1: test name
function build_s3client_url() {
    local test_name="$1"
    echo "blobstore://${S3CLIENT_HOST}/${S3CLIENT_PATH_PREFIX}/${test_name}?${S3CLIENT_QUERY_STR}"
}

# Run s3client command with proper configuration
# $1: integrity check (true/false)
# $@: command arguments
function run_s3client() {
    local integrity_check="${1:-false}"
    shift
    
    local cmd_args=()
    cmd_args+=("${TEST_BUILD_DIR}/bin/s3client")
    cmd_args+=("--knob_http_verbose_level=${S3CLIENT_HTTP_VERBOSE_LEVEL}")
    
    # Only use AWS KMS encryption with real S3
    if [[ "${S3CLIENT_USE_S3}" == "true" ]]; then
        cmd_args+=("--knob_blobstore_encryption_type=aws:kms")
    fi
    
    cmd_args+=("--knob_blobstore_enable_object_integrity_check=${integrity_check}")
    
    # Only add TLS CA file if it's not empty
    if [[ -n "${S3CLIENT_TLS_CA_FILE}" ]]; then
        cmd_args+=("--tls-ca-file" "${S3CLIENT_TLS_CA_FILE}")
    fi
    
    cmd_args+=("--blob-credentials" "${S3CLIENT_CREDENTIALS_FILE}")
    cmd_args+=("--log" "--logdir" "${TEST_SCRATCH_DIR}/logs")
    
    # Add remaining arguments
    cmd_args+=("$@")
    
    # Execute the command
    "${cmd_args[@]}"
}

# Upload, download, and verify a file
# $1: test name
# $2: source file
# $3: downloaded file
# $4: integrity check (optional, default: true for S3, false for SeaweedFS)
function s3client_upload_download_file() {
    local test_name="$1"
    local source_file="$2"
    local downloaded_file="$3"
    local integrity_check="${4:-}"
    
    # Set default integrity check based on backend
    if [[ -z "${integrity_check}" ]]; then
        if [[ "${S3CLIENT_USE_S3}" == "true" ]]; then
            integrity_check="true"
        else
            integrity_check="false"
        fi
    fi
    
    local url
    url=$(build_s3client_url "${test_name}")
    
    # Clean up any existing file (for S3)
    if [[ "${S3CLIENT_USE_S3}" == "true" ]]; then
        run_s3client "${integrity_check}" rm "${url}" || true
    fi
    
    # Upload
    assert_success "Upload ${source_file} to ${url}" \
        run_s3client "${integrity_check}" cp "${source_file}" "${url}"
    
    # Download
    assert_success "Download ${url} to ${downloaded_file}" \
        run_s3client "${integrity_check}" cp "${url}" "${downloaded_file}"
    
    # Verify content
    assert_success "Files should be identical" \
        diff "${source_file}" "${downloaded_file}"
    
    # Cleanup
    assert_success "Remove ${url}" \
        run_s3client "${integrity_check}" rm "${url}"
}

# Test file upload and download
function test_s3client_file_upload_download() {
    local test_dir
    test_dir=$(create_test_dir "file_test")
    
    local test_file_up="${test_dir}/test_file.txt"
    local test_file_down="${test_dir}/test_file_downloaded.txt"
    
    # Create test file
    date -Iseconds > "${test_file_up}"
    
    s3client_upload_download_file "file_test" "${test_file_up}" "${test_file_down}"
}

# Test directory upload and download
function test_s3client_directory_upload_download() {
    local test_dir
    test_dir=$(create_test_dir "dir_test")
    
    local test_dir_up="${test_dir}/upload"
    local test_dir_down="${test_dir}/download"
    
    mkdir -p "${test_dir_up}" "${test_dir_down}"
    
    # Create test directory structure
    date -Iseconds > "${test_dir_up}/file1.txt"
    date -Iseconds > "${test_dir_up}/file2.txt"
    mkdir "${test_dir_up}/subdir"
    date -Iseconds > "${test_dir_up}/subdir/file3.txt"
    
    local url
    url=$(build_s3client_url "dir_test")
    
    local integrity_check
    if [[ "${S3CLIENT_USE_S3}" == "true" ]]; then
        integrity_check="true"
        # Clean up any existing directory
        run_s3client "${integrity_check}" rm "${url}" || true
    else
        integrity_check="false"
    fi
    
    # Upload directory
    assert_success "Upload directory ${test_dir_up} to ${url}" \
        run_s3client "${integrity_check}" cp "${test_dir_up}" "${url}"
    
    # Download directory
    assert_success "Download ${url} to ${test_dir_down}" \
        run_s3client "${integrity_check}" cp "${url}" "${test_dir_down}"
    
    # Verify structure
    assert_file_exists "${test_dir_down}/upload/file1.txt"
    assert_file_exists "${test_dir_down}/upload/file2.txt"
    assert_file_exists "${test_dir_down}/upload/subdir/file3.txt"
    
    # Cleanup
    assert_success "Remove ${url}" \
        run_s3client "${integrity_check}" rm "${url}"
}

# Test listing functionality
function test_s3client_listing() {
    local test_dir
    test_dir=$(create_test_dir "ls_test")
    
    # Create test files
    for i in {1..3}; do
        date -Iseconds > "${test_dir}/file${i}.txt"
    done
    
    local url
    url=$(build_s3client_url "ls_test")
    
    local integrity_check
    if [[ "${S3CLIENT_USE_S3}" == "true" ]]; then
        integrity_check="true"
        # Clean up any existing files
        run_s3client "${integrity_check}" rm "${url}" || true
    else
        integrity_check="false"
    fi
    
    # Upload test files
    assert_success "Upload test files to ${url}" \
        run_s3client "${integrity_check}" cp "${test_dir}" "${url}"
    
    # Test basic listing
    local output
    output=$(run_s3client "${integrity_check}" ls "${url}" 2>&1)
    
    # Verify files are listed
    for i in {1..3}; do
        assert_contains "${output}" "file${i}.txt"
    done
    
    # Test recursive listing
    output=$(run_s3client "${integrity_check}" ls --recursive "${url}" 2>&1)
    
    # Verify files are still listed
    for i in {1..3}; do
        assert_contains "${output}" "file${i}.txt"
    done
    
    # Test with max keys parameter
    output=$(run_s3client "${integrity_check}" --knob_blobstore_list_max_keys_per_page=5 ls "${url}" 2>&1)
    
    # Should still work with pagination
    for i in {1..3}; do
        assert_contains "${output}" "file${i}.txt"
    done
    
    # Cleanup
    assert_success "Remove test files" \
        run_s3client "${integrity_check}" rm "${url}"
}

# Test file upload/download without integrity check (S3 only)
function test_s3client_no_integrity_check() {
    if [[ "${S3CLIENT_USE_S3}" != "true" ]]; then
        skip_test "Integrity check test only runs with S3"
        return 0
    fi
    
    local test_dir
    test_dir=$(create_test_dir "no_integrity_test")
    
    local test_file_up="${test_dir}/test_file.txt"
    local test_file_down="${test_dir}/test_file_downloaded.txt"
    
    # Create test file
    date -Iseconds > "${test_file_up}"
    
    # Test with integrity check disabled
    s3client_upload_download_file "no_integrity_test" "${test_file_up}" "${test_file_down}" "false"
}

# Test error handling for non-existent resources
function test_s3client_error_handling() {
    local url
    url=$(build_s3client_url "nonexistent")
    
    local integrity_check
    if [[ "${S3CLIENT_USE_S3}" == "true" ]]; then
        integrity_check="true"
    else
        integrity_check="false"
    fi
    
    # Test listing non-existent path
    local output
    output=$(run_s3client "${integrity_check}" ls "${url}" 2>&1) || true
    
    if [[ "${S3CLIENT_USE_S3}" == "true" ]]; then
        # For S3, should succeed but return empty listing
        assert_contains "${output}" "Contents of"
    else
        # For SeaweedFS, should also succeed
        assert_contains "${output}" "Contents of"
    fi
}

# Setup function for S3Client test suite
function setup_s3client_tests() {
    # Arguments are passed from the test runner
    load_s3client_config "$@"
    
    # Create logs directory
    mkdir -p "${TEST_SCRATCH_DIR}/logs"
    
    # Register cleanup for fixtures
    if [[ "${S3CLIENT_USE_S3}" == "true" ]]; then
        register_cleanup "cleanup_aws_fixture"
    else
        register_cleanup "cleanup_seaweedfs_fixture"
    fi
}

# Cleanup function for AWS fixture
function cleanup_aws_fixture() {
    if type shutdown_aws &> /dev/null; then
        shutdown_aws "${TEST_SCRATCH_DIR}"
    fi
}

# Cleanup function for SeaweedFS fixture
function cleanup_seaweedfs_fixture() {
    if type shutdown_weed &> /dev/null; then
        shutdown_weed "${TEST_SCRATCH_DIR}"
    fi
}

# Main test suite function
function run_s3client_test_suite() {
    local tests=(
        "test_s3client_file_upload_download"
        "test_s3client_directory_upload_download"
        "test_s3client_listing"
        "test_s3client_error_handling"
    )
    
    # Add S3-specific tests
    if [[ "${S3CLIENT_USE_S3}" == "true" ]]; then
        tests+=("test_s3client_no_integrity_check")
    fi
    
    run_test_suite "S3Client" "setup_s3client_tests" "" "${tests[@]}"
} 