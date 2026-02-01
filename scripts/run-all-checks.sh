#!/usr/bin/env bash
#
# rms-cloud-tasks - Run All Checks Script
#
# This script runs linting, type checking, tests, and documentation build
# for the rms-cloud-tasks project.
#
# Usage:
#   ./scripts/run-all-checks.sh [options]
#
# Options:
#   -p, --parallel    Run code checks and docs build in parallel (faster, default)
#   -s, --sequential Run all checks sequentially (easier to debug)
#   -c, --code       Run only code checks (ruff, mypy, pytest)
#   -d, --docs       Run only documentation build (Sphinx)
#   -h, --help       Show this help message
#
# Code checks (run from project root with venv activated):
#   - ruff check (src, tests, examples)
#   - ruff format --check (src, tests, examples)
#   - mypy (src, tests)
#   - pytest (tests)
#
# Exit codes:
#   0 - All checks passed
#   1 - One or more checks failed
#

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
RESET='\033[0m'

# Default options
PARALLEL=true
RUN_CODE=false
RUN_DOCS=false
SCOPE_SPECIFIED=false

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV="$PROJECT_ROOT/venv"

# Track failures
FAILED_CHECKS=()
EXIT_CODE=0

# Create temp directory for parallel output
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

# Print functions
print_header() {
    echo -e "\n${BOLD}${BLUE}===================================================${RESET}"
    echo -e "${BOLD}${BLUE}  $1${RESET}"
    echo -e "${BOLD}${BLUE}===================================================${RESET}\n"
}

print_section() {
    echo -e "\n${BOLD}${YELLOW}>>> $1${RESET}\n"
}

print_success() {
    echo -e "${GREEN}✓${RESET} $1"
}

print_error() {
    echo -e "${RED}✗${RESET} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${RESET} $1"
}

# Show usage
show_usage() {
    sed -n '/^# Usage:/,/^$/p' "$0" | sed 's/^# //g' | sed 's/^#//g'
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--parallel)
            PARALLEL=true
            shift
            ;;
        -s|--sequential)
            PARALLEL=false
            shift
            ;;
        -c|--code)
            RUN_CODE=true
            SCOPE_SPECIFIED=true
            shift
            ;;
        -d|--docs)
            RUN_DOCS=true
            SCOPE_SPECIFIED=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo -e "${RED}Error: Unknown option: $1${RESET}" >&2
            show_usage
            exit 1
            ;;
    esac
done

# If no scope flag was given, run all checks
if [ "$SCOPE_SPECIFIED" = false ]; then
    RUN_CODE=true
    RUN_DOCS=true
fi

# Start timer
START_TIME=$(date +%s)

print_header "rms-cloud-tasks - Running All Checks"

if [ "$PARALLEL" = true ]; then
    print_info "Running checks in PARALLEL mode"
else
    print_info "Running checks in SEQUENTIAL mode"
fi

# Function to run code checks (ruff, mypy, pytest)
run_code_checks() {
    local check_name="Code Checks"
    local output_file="${1:-}"
    local status_file="${2:-}"

    if [ -n "$output_file" ]; then
        exec > "$output_file" 2>&1
    fi

    print_section "$check_name"

    cd "$PROJECT_ROOT"

    if [ ! -f "$VENV/bin/activate" ]; then
        print_error "Virtual environment not found at venv/"
        [ -n "$status_file" ] && echo "Code - Virtual environment not found" >> "$status_file"
        return 1
    fi

    source "$VENV/bin/activate"

    local failed=false
    local failed_checks=""

    # Ruff check
    print_info "Running ruff check..."
    if python -m ruff check src tests examples; then
        print_success "Ruff check passed"
    else
        print_error "Ruff check failed"
        failed=true
        failed_checks="${failed_checks}Code - Ruff check"$'\n'
    fi

    # Ruff format check
    print_info "Running ruff format --check..."
    if python -m ruff format --check src tests examples; then
        print_success "Ruff format check passed"
    else
        print_error "Ruff format check failed"
        failed=true
        failed_checks="${failed_checks}Code - Ruff format"$'\n'
    fi

    # Mypy
    print_info "Running mypy..."
    if python -m mypy src tests; then
        print_success "Mypy passed"
    else
        print_error "Mypy failed"
        failed=true
        failed_checks="${failed_checks}Code - Mypy"$'\n'
    fi

    # Pytest
    print_info "Running pytest..."
    if python -m pytest tests -q; then
        print_success "Pytest passed"
    else
        print_error "Pytest failed"
        failed=true
        failed_checks="${failed_checks}Code - Pytest"$'\n'
    fi

    deactivate 2>/dev/null || true

    if [ "$failed" = true ]; then
        if [ -n "$status_file" ]; then
            echo -n "$failed_checks" >> "$status_file"
        else
            while IFS= read -r line; do
                [ -n "$line" ] && FAILED_CHECKS+=("$line")
            done <<< "$failed_checks"
        fi
        return 1
    fi

    return 0
}

# Function to run documentation build
run_docs_build() {
    local check_name="Documentation Build"
    local output_file="${1:-}"
    local status_file="${2:-}"

    if [ -n "$output_file" ]; then
        exec > "$output_file" 2>&1
    fi

    print_section "$check_name"

    cd "$PROJECT_ROOT"

    if [ ! -f "$VENV/bin/activate" ]; then
        print_error "Virtual environment not found at venv/"
        [ -n "$status_file" ] && echo "Documentation - Virtual environment not found" >> "$status_file"
        return 1
    fi

    source "$VENV/bin/activate"

    local failed=false

    print_info "Building documentation (warnings treated as errors)..."
    if (cd docs && make clean && make html SPHINXOPTS="-W"); then
        print_success "Sphinx build passed (no errors or warnings)"
    else
        print_error "Sphinx build failed (errors or warnings present)"
        failed=true
    fi

    deactivate 2>/dev/null || true

    if [ "$failed" = true ]; then
        if [ -n "$status_file" ]; then
            echo "Documentation - Sphinx build" >> "$status_file"
        else
            FAILED_CHECKS+=("Documentation - Sphinx build")
        fi
        return 1
    fi

    return 0
}

# Run checks based on mode
if [ "$PARALLEL" = true ] && [ "$RUN_CODE" = true ] && [ "$RUN_DOCS" = true ]; then
    # Run code and docs in parallel
    code_output="$TEMP_DIR/code.log"
    code_status="$TEMP_DIR/code.status"
    docs_output="$TEMP_DIR/docs.log"
    docs_status="$TEMP_DIR/docs.status"

    print_info "Running code checks and docs build in parallel, please wait..."
    run_code_checks "$code_output" "$code_status" &
    code_pid=$!
    run_docs_build "$docs_output" "$docs_status" &
    docs_pid=$!

    if ! wait "$code_pid"; then
        EXIT_CODE=1
    fi
    if ! wait "$docs_pid"; then
        EXIT_CODE=1
    fi

    for status_file in "$code_status" "$docs_status"; do
        if [ -f "$status_file" ]; then
            while IFS= read -r line; do
                [ -n "$line" ] && FAILED_CHECKS+=("$line")
            done < "$status_file"
        fi
    done

    echo ""
    [ -f "$code_output" ] && cat "$code_output"
    [ -f "$docs_output" ] && cat "$docs_output"
else
    # Sequential (or only one scope)
    if [ "$RUN_CODE" = true ]; then
        if ! run_code_checks; then
            EXIT_CODE=1
        fi
    fi

    if [ "$RUN_DOCS" = true ]; then
        if ! run_docs_build; then
            EXIT_CODE=1
        fi
    fi
fi

# Calculate elapsed time
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
ELAPSED_SECONDS=$((ELAPSED % 60))

# Print summary
print_header "Summary"

if [ ${#FAILED_CHECKS[@]} -eq 0 ]; then
    print_success "All checks passed!"
    echo -e "${GREEN}${BOLD}✓ SUCCESS${RESET} - All checks completed successfully"
else
    print_error "Some checks failed:"
    for check in "${FAILED_CHECKS[@]}"; do
        echo -e "  ${RED}✗${RESET} $check"
    done
    echo -e "${RED}${BOLD}✗ FAILURE${RESET} - ${#FAILED_CHECKS[@]} check(s) failed"
fi

echo ""
print_info "Total time: ${MINUTES}m ${ELAPSED_SECONDS}s"
echo ""

exit $EXIT_CODE
