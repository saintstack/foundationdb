#!/bin/bash
# Test runner framework for FDB bash tests
# Provides standardized test execution, result tracking, and cleanup

# Guard against multiple sourcing
if [[ "${TEST_RUNNER_LOADED:-false}" == "true" ]]; then
    return 0
fi

set -euo pipefail

# Test runner state
declare TEST_RUNNER_LOADED=true
declare TEST_RUNNER_INITIALIZED=false
declare TEST_RESULTS=()
declare TEST_CLEANUP_FUNCTIONS=()
declare TEST_SCRATCH_DIR=""
declare TEST_BUILD_DIR=""

# Colors for output (only declare if not already set)
if [[ -z "${RED:-}" ]]; then
    readonly RED='\033[0;31m'
    readonly GREEN='\033[0;32m'
    readonly YELLOW='\033[1;33m'
    readonly NC='\033[0m' # No Color
fi

# Initialize the test runner
# $1: build directory
# $2: scratch directory (optional)
function init_test_runner() {
    if [[ $# -eq 0 ]]; then
        echo "ERROR: init_test_runner requires at least one argument (build directory)" >&2
        return 1
    fi
    
    local build_dir="$1"
    local scratch_dir="${2:-${TMPDIR:-/tmp}}"
    
    if [[ "${TEST_RUNNER_INITIALIZED}" == "true" ]]; then
        return 0
    fi
    
    # Validate build directory
    if [[ ! -d "${build_dir}" ]]; then
        echo "ERROR: Build directory ${build_dir} does not exist" >&2
        return 1
    fi
    
    TEST_BUILD_DIR="${build_dir}"
    TEST_SCRATCH_DIR="${scratch_dir}/test_runner_$$"
    
    # Create test scratch directory
    mkdir -p "${TEST_SCRATCH_DIR}"
    
    # Setup cleanup trap
    trap cleanup_test_runner EXIT
    
    TEST_RUNNER_INITIALIZED=true
    
    # Source common utilities
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    if [[ -f "${script_dir}/tests_common.sh" ]]; then
        source "${script_dir}/tests_common.sh"
    fi
    
    log "Test runner initialized"
    log "Build directory: ${TEST_BUILD_DIR}"
    log "Scratch directory: ${TEST_SCRATCH_DIR}"
}

# Register a cleanup function to be called on exit
# $1: function name
function register_cleanup() {
    local cleanup_func="$1"
    TEST_CLEANUP_FUNCTIONS+=("${cleanup_func}")
}

# Run a single test and track results
# $1: test name
# $2: test function
# $@: additional arguments to pass to test function
function run_test() {
    local test_name="$1"
    local test_func="$2"
    shift 2
    
    log "Running test: ${test_name}"
    
    local start_time
    start_time=$(date +%s)
    
    if "${test_func}" "$@"; then
        local end_time
        end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        TEST_RESULTS+=("${test_name}:PASS:${duration}")
        echo -e "${GREEN}✓${NC} ${test_name} (${duration}s)"
        if type log_test_result &>/dev/null; then
            log_test_result 0 "${test_name}"
        fi
    else
        local end_time
        end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        TEST_RESULTS+=("${test_name}:FAIL:${duration}")
        echo -e "${RED}✗${NC} ${test_name} (${duration}s)"
        if type log_test_result &>/dev/null; then
            log_test_result 1 "${test_name}"
        fi
    fi
}

# Run a test suite (collection of tests)
# $1: suite name
# $2: suite setup function (optional)
# $3: suite teardown function (optional)
# $@: test functions to run
function run_test_suite() {
    local suite_name="$1"
    local setup_func="${2:-}"
    local teardown_func="${3:-}"
    shift 3
    
    log "Starting test suite: ${suite_name}"
    
    # Run setup if provided
    if [[ -n "${setup_func}" ]]; then
        log "Running suite setup: ${setup_func}"
        # Pass all original arguments to setup function
        if ! "${setup_func}" "${TEST_BUILD_DIR}" "${TEST_SCRATCH_DIR%/*}"; then
            err "Suite setup failed: ${setup_func}"
            return 1
        fi
    fi
    
    # Register teardown for cleanup
    if [[ -n "${teardown_func}" ]]; then
        register_cleanup "${teardown_func}"
    fi
    
    # Run each test
    for test_func in "$@"; do
        run_test "${test_func}" "${test_func}"
    done
    
    log "Completed test suite: ${suite_name}"
}

# Print test results summary
function print_test_results() {
    local total_tests=0
    local passed_tests=0
    local failed_tests=0
    local total_duration=0
    
    echo
    echo "=================================="
    echo "Test Results Summary"
    echo "=================================="
    
    for result in "${TEST_RESULTS[@]}"; do
        IFS=':' read -r test_name status duration <<< "${result}"
        total_tests=$((total_tests + 1))
        total_duration=$((total_duration + duration))
        
        if [[ "${status}" == "PASS" ]]; then
            passed_tests=$((passed_tests + 1))
            echo -e "${GREEN}✓${NC} ${test_name} (${duration}s)"
        else
            failed_tests=$((failed_tests + 1))
            echo -e "${RED}✗${NC} ${test_name} (${duration}s)"
        fi
    done
    
    echo "=================================="
    echo "Total: ${total_tests}, Passed: ${passed_tests}, Failed: ${failed_tests}"
    echo "Total Duration: ${total_duration}s"
    echo "=================================="
    
    return "${failed_tests}"
}

# Cleanup function called on exit
function cleanup_test_runner() {
    if [[ "${TEST_RUNNER_INITIALIZED}" != "true" ]]; then
        return 0
    fi
    
    log "Running cleanup functions"
    
    # Run registered cleanup functions in reverse order
    for ((i=${#TEST_CLEANUP_FUNCTIONS[@]}-1; i>=0; i--)); do
        local cleanup_func="${TEST_CLEANUP_FUNCTIONS[i]}"
        log "Running cleanup: ${cleanup_func}"
        "${cleanup_func}" || true  # Don't fail cleanup on error
    done
    
    # Clean up test scratch directory
    if [[ -n "${TEST_SCRATCH_DIR}" && -d "${TEST_SCRATCH_DIR}" ]]; then
        log "Cleaning up test scratch directory: ${TEST_SCRATCH_DIR}"
        rm -rf "${TEST_SCRATCH_DIR}"
    fi
    
    log "Test runner cleanup completed"
}

# Create a temporary test directory
# $1: directory name
function create_test_dir() {
    local dir_name="$1"
    local test_dir="${TEST_SCRATCH_DIR}/${dir_name}"
    mkdir -p "${test_dir}"
    echo "${test_dir}"
}

# Assert that a command succeeds
# $1: command description
# $@: command to run
function assert_success() {
    local description="$1"
    shift
    
    if ! "$@"; then
        err "Assertion failed: ${description}"
        return 1
    fi
}

# Assert that a command fails
# $1: command description
# $@: command to run
function assert_failure() {
    local description="$1"
    shift
    
    if "$@"; then
        err "Assertion failed (expected failure): ${description}"
        return 1
    fi
}

# Assert that a file exists
# $1: file path
function assert_file_exists() {
    local file_path="$1"
    
    if [[ ! -f "${file_path}" ]]; then
        err "Assertion failed: File does not exist: ${file_path}"
        return 1
    fi
}

# Assert that a directory exists
# $1: directory path
function assert_dir_exists() {
    local dir_path="$1"
    
    if [[ ! -d "${dir_path}" ]]; then
        err "Assertion failed: Directory does not exist: ${dir_path}"
        return 1
    fi
}

# Assert that two strings are equal
# $1: expected
# $2: actual
function assert_equals() {
    local expected="$1"
    local actual="$2"
    
    if [[ "${expected}" != "${actual}" ]]; then
        err "Assertion failed: Expected '${expected}', got '${actual}'"
        return 1
    fi
}

# Assert that a string contains a substring
# $1: haystack
# $2: needle
function assert_contains() {
    local haystack="$1"
    local needle="$2"
    
    if [[ "${haystack}" != *"${needle}"* ]]; then
        err "Assertion failed: '${haystack}' does not contain '${needle}'"
        return 1
    fi
}

# Skip a test with a reason
# $1: reason
function skip_test() {
    local reason="$1"
    echo -e "${YELLOW}⚠${NC} Test skipped: ${reason}"
    return 0
}

# Check if we're running in a specific environment
function is_ci() {
    [[ -n "${CI:-}" ]] || [[ -n "${GITHUB_ACTIONS:-}" ]] || [[ -n "${JENKINS_URL:-}" ]]
}

function is_macos() {
    [[ "$(uname)" == "Darwin" ]]
}

function is_linux() {
    [[ "$(uname)" == "Linux" ]]
}

# Default implementations of log and err if not available from tests_common.sh
if ! type log &>/dev/null; then
    function log() {
        printf "%s %s\n" "$(date -Iseconds)" "${1}"
    }
fi

if ! type err &>/dev/null; then
    function err() {
        echo "$(date -Iseconds) ERROR: ${*}" >&2
    }
fi 