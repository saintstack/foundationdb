#!/bin/bash
# Configuration module for s3client tests

# Global configuration variables
declare TEST_SCRATCH_DIR
declare USE_S3
declare TLS_CA_FILE
declare HTTP_VERBOSE_LEVEL=2

# S3/SeaweedFS connection details
declare host
declare bucket
declare region
declare blob_credentials_file
declare query_str
declare path_prefix

# Initialize configuration
function init_config() {
    local build_dir="$1"
    local scratch_dir="${2:-${TMPDIR:-/tmp}}"
    
    # Initialize HTTP verbose level
    HTTP_VERBOSE_LEVEL="${HTTP_VERBOSE_LEVEL:-2}"
    export HTTP_VERBOSE_LEVEL
    
    # Determine if we're using S3 or SeaweedFS
    USE_S3="${USE_S3:-$( if [[ -n "${OKTETO_NAMESPACE+x}" ]]; then echo "true" ; else echo "false"; fi )}"
    export USE_S3
    
    # Set TLS CA file for S3
    if [[ "${USE_S3}" == "true" ]]; then
        find_tls_ca_file
    else
        TLS_CA_FILE=""
    fi
    export TLS_CA_FILE
    
    # Validate build directory
    if [[ ! -d "${build_dir}" ]]; then
        err "${build_dir} is not a directory"
        return 1
    fi
    
    return 0
}

# Find TLS CA file for S3 connections
function find_tls_ca_file() {
    if [[ -n "${TLS_CA_FILE:-}" ]]; then
        return 0
    fi
    
    local ca_locations=(
        "/etc/pki/tls/cert.pem"
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"
        "/etc/ssl/certs/ca-certificates.crt"
        "/etc/pki/tls/certs/ca-bundle.crt"
        "/etc/ssl/cert.pem"
    )
    
    for ca_file in "${ca_locations[@]}"; do
        if [[ -f "${ca_file}" ]]; then
            TLS_CA_FILE="${ca_file}"
            return 0
        fi
    done
    
    TLS_CA_FILE=""
}

# Setup S3 configuration
function setup_s3_config() {
    local build_dir="$1"
    local scratch_dir="$2"
    
    log "Setting up S3 configuration"
    
    # Source AWS fixture
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    
    if ! source "${script_dir}/aws_fixture.sh"; then
        err "Failed to source aws_fixture.sh"
        return 1
    fi
    
    if ! TEST_SCRATCH_DIR=$(create_aws_dir "${scratch_dir}"); then
        err "Failed creating AWS test directory"
        return 1
    fi
    
    # Setup AWS configuration
    if ! readarray -t configs < <(aws_setup "${build_dir}" "${TEST_SCRATCH_DIR}"); then
        err "Failed aws_setup"
        return 1
    fi
    
    host="${configs[0]}"
    bucket="${configs[1]}"
    blob_credentials_file="${configs[2]}"
    region="${configs[3]}"
    
    query_str="bucket=${bucket}&region=${region}&secure_connection=1"
    path_prefix="bulkload/test/s3client"
}

# Setup SeaweedFS configuration
function setup_seaweedfs_config() {
    local build_dir="$1"
    local scratch_dir="$2"
    
    log "Setting up SeaweedFS configuration"
    
    # Source SeaweedFS fixture
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    
    if ! source "${script_dir}/seaweedfs_fixture.sh"; then
        err "Failed to source seaweedfs_fixture.sh"
        return 1
    fi
    
    if ! TEST_SCRATCH_DIR=$(create_weed_dir "${scratch_dir}"); then
        err "Failed to create SeaweedFS test directory"
        return 1
    fi
    
    if ! host=$(run_weed "${scratch_dir}" "${TEST_SCRATCH_DIR}"); then
        err "Failed to run SeaweedFS"
        return 1
    fi
    
    bucket="${SEAWEED_BUCKET}"
    region="all_regions"
    blob_credentials_file="${TEST_SCRATCH_DIR}/blob_credentials.json"
    touch "${blob_credentials_file}"
    
    query_str="bucket=${bucket}&region=${region}&secure_connection=0"
    path_prefix="s3client"
}

# Parse command line arguments
function parse_args() {
    local blob_credentials_file_arg=""
    local bucket_arg=""
    local region_arg=""
    local host_arg=""
    local extra_url_params=""
    local positional=()
    
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --blob-credentials-file)
                blob_credentials_file_arg="$2"
                shift 2
                ;;
            --region)
                region_arg="$2"
                shift 2
                ;;
            --bucket)
                bucket_arg="$2"
                shift 2
                ;;
            --host)
                host_arg="$2"
                shift 2
                ;;
            --extra-url-params)
                extra_url_params="$2"
                shift 2
                ;;
            --)
                shift
                break
                ;;
            -*)
                echo "Unknown option: $1" >&2
                return 1
                ;;
            *)
                positional+=("$1")
                shift
                ;;
        esac
    done
    
    # Set remaining positional arguments
    set -- "${positional[@]+"${positional[@]}"}"
    
    # Validate argument count
    if (( $# < 1 )) || (( $# > 2 )); then
        echo "ERROR: ${0} requires the fdb build directory as its first argument" >&2
        echo "and optionally a scratch directory as the second argument" >&2
        return 1
    fi
    
    # Export parsed arguments
    export PARSED_BUILD_DIR="$1"
    export PARSED_SCRATCH_DIR="${2:-${TMPDIR:-/tmp}}"
    export PARSED_BLOB_CREDENTIALS_FILE="${blob_credentials_file_arg}"
    export PARSED_BUCKET="${bucket_arg}"
    export PARSED_REGION="${region_arg}"
    export PARSED_HOST="${host_arg}"
    export PARSED_EXTRA_URL_PARAMS="${extra_url_params}"
    
    return 0
}

# Override configuration with command line arguments
function apply_arg_overrides() {
    if [[ -n "${PARSED_BLOB_CREDENTIALS_FILE}" ]]; then
        if [[ -n "${PARSED_BUCKET}" && -n "${PARSED_REGION}" && -n "${PARSED_HOST}" ]]; then
            log "Using explicit configuration from command line"
            host="${PARSED_HOST}"
            bucket="${PARSED_BUCKET}"
            region="${PARSED_REGION}"
            blob_credentials_file="${PARSED_BLOB_CREDENTIALS_FILE}"
            
            # Rebuild query string
            if [[ -n "${PARSED_EXTRA_URL_PARAMS}" ]]; then
                query_str="bucket=${bucket}&region=${region}${PARSED_EXTRA_URL_PARAMS}&secure_connection=1"
            else
                query_str="bucket=${bucket}&region=${region}&secure_connection=1"
            fi
        else
            echo "ERROR: If any of --host, --bucket, --region, or --blob-credentials-file are set, then all must be set." >&2
            return 1
        fi
    fi
} 