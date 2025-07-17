#!/bin/bash
# S3Client test script using modular framework
# This is a much cleaner version of the original s3client_test.sh

set -euo pipefail

# Get script directory
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source the test framework and S3Client tests
source "${script_dir}/lib/test_runner.sh"
source "${script_dir}/lib/s3client_tests.sh"

# Main function
function main() {
    # Initialize test runner
    init_test_runner "$@"
    
    # Run S3Client test suite
    run_s3client_test_suite "$@"
    
    # Print results and exit with appropriate code
    if print_test_results; then
        log "All tests passed!"
        exit 0
    else
        log "Some tests failed!"
        exit 1
    fi
}

# Run main function with all arguments
main "$@" 