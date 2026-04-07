#!/bin/bash
#
# Wrapper script for ALMA DAGMAN imaging workflow
# This script validates the environment, generates the DAG, and optionally submits it
#

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# Source configuration
source "${SCRIPT_DIR}/config.sh"

# Function to print colored messages
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Run ALMA DAGMAN imaging workflow

OPTIONS:
    -t, --test N         Test mode: only process N channels (default: all)
    -s, --submit         Automatically submit DAG after generation
    -c, --check          Only check environment, don't generate DAG
    -h, --help           Show this help message

EXAMPLES:
    $0 --check                    # Check environment only
    $0 --test 10                  # Generate DAG for 10 channels (test mode)
    $0 --test 10 --submit         # Generate and submit DAG for 10 channels
    $0 --submit                   # Generate and submit full DAG (238011 channels)

EOF
    exit 1
}

# Parse command line arguments
TEST_MODE=false
TEST_CHANNELS=0
AUTO_SUBMIT=false
CHECK_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--test)
            TEST_MODE=true
            TEST_CHANNELS="$2"
            shift 2
            ;;
        -s|--submit)
            AUTO_SUBMIT=true
            shift
            ;;
        -c|--check)
            CHECK_ONLY=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Print header
echo "========================================"
echo "  ALMA DAGMAN Imaging Workflow"
echo "========================================"
echo ""

# Check dependencies
print_info "Checking dependencies..."

# Check for condor commands
if ! command -v condor_submit_dag &> /dev/null; then
    print_error "condor_submit_dag not found. Is HTCondor installed?"
    exit 1
fi
print_success "HTCondor found"

# Check for python3
if ! command -v python3 &> /dev/null; then
    print_error "python3 not found"
    exit 1
fi
print_success "Python3 found"

# Check for apptainer
if ! command -v apptainer &> /dev/null; then
    print_error "apptainer not found"
    exit 1
fi
print_success "Apptainer found"

echo ""
print_info "Validating paths from config..."

# Check if input MS exists
if [ ! -d "${INPUT_MS}" ]; then
    print_warning "Input MS not found: ${INPUT_MS}"
    print_warning "This may be expected if running on a submit node"
else
    print_success "Input MS found: ${INPUT_MS}"
fi

# Check if container exists
if [ ! -f "${CONTAINER_SIF}" ]; then
    print_warning "Container not found: ${CONTAINER_SIF}"
    print_warning "Make sure this path is correct for your environment"
else
    print_success "Container found: ${CONTAINER_SIF}"
fi

# Check/create scratch directory
if [ ! -d "${SCRATCH_DIR}" ]; then
    print_warning "Scratch directory does not exist: ${SCRATCH_DIR}"
    read -p "Create it? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        mkdir -p "${SCRATCH_DIR}"
        print_success "Created scratch directory: ${SCRATCH_DIR}"
    else
        print_error "Scratch directory required. Exiting."
        exit 1
    fi
else
    print_success "Scratch directory exists: ${SCRATCH_DIR}"
fi

# Check/create output directory
if [ ! -d "${OUTPUT_DIR}" ]; then
    print_warning "Output directory does not exist: ${OUTPUT_DIR}"
    read -p "Create it? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        mkdir -p "${OUTPUT_DIR}"
        print_success "Created output directory: ${OUTPUT_DIR}"
    else
        print_error "Output directory required. Exiting."
        exit 1
    fi
else
    print_success "Output directory exists: ${OUTPUT_DIR}"
fi

# Check required scripts
for script in split_channel.py split_channel.sh cleanup.sh tclean.py tclean.sub; do
    if [ ! -f "${SCRIPT_DIR}/${script}" ]; then
        print_error "Required script not found: ${script}"
        exit 1
    fi
done
print_success "All required scripts found"

# Make scripts executable
chmod +x "${SCRIPT_DIR}/split_channel.sh"
chmod +x "${SCRIPT_DIR}/cleanup.sh"
print_success "Scripts are executable"

echo ""

# Exit if check-only mode
if [ "$CHECK_ONLY" = true ]; then
    print_success "Environment check completed successfully!"
    exit 0
fi

# Generate DAG
print_info "Generating DAG file..."

if [ "$TEST_MODE" = true ]; then
    print_warning "TEST MODE: Processing only ${TEST_CHANNELS} channels"
    # Temporarily modify config for test
    python3 << EOF
import os
import sys
sys.path.insert(0, '${SCRIPT_DIR}')

# Override N_CHANNELS for test mode
N_CHANNELS = ${TEST_CHANNELS}

# Create logs directory
os.makedirs('logs', exist_ok=True)

# Generate DAG
with open('tclean_dag.dag', 'w') as fptr:
    for ii in range(N_CHANNELS):
        chan_name = f"chan{ii}"
        fptr.write(f"JOB tclean_{chan_name} tclean.sub\n")
        fptr.write(f"VARS tclean_{chan_name} channum=\"{ii}\" channame=\"{chan_name}\"\n")
        fptr.write(f"SCRIPT PRE tclean_{chan_name} split_channel.sh {ii} {chan_name}\n")
        fptr.write(f"SCRIPT POST tclean_{chan_name} cleanup.sh {ii} {chan_name}\n")
        fptr.write(f"\n")

print(f"Generated DAG with {N_CHANNELS} jobs")
EOF
else
    python3 generate_dagman.py
fi

if [ ! -f "tclean_dag.dag" ]; then
    print_error "Failed to generate DAG file"
    exit 1
fi

# Count jobs
NUM_JOBS=$(grep -c "^JOB" tclean_dag.dag)
print_success "DAG file generated: tclean_dag.dag"
print_info "Total jobs: ${NUM_JOBS}"

echo ""

# Submit DAG if requested
if [ "$AUTO_SUBMIT" = true ]; then
    print_info "Submitting DAG to HTCondor..."

    condor_submit_dag tclean_dag.dag

    if [ $? -eq 0 ]; then
        print_success "DAG submitted successfully!"
        echo ""
        print_info "Monitor your DAG with:"
        echo "    condor_q"
        echo "    tail -f tclean_dag.dag.dagman.out"
        echo ""
        print_info "Check DAG status with:"
        echo "    condor_q -dag"
    else
        print_error "Failed to submit DAG"
        exit 1
    fi
else
    print_info "DAG file ready. To submit, run:"
    echo "    condor_submit_dag tclean_dag.dag"
    echo ""
    print_info "Or run this script with --submit flag:"
    echo "    $0 --submit"
fi

echo ""
print_success "Workflow setup complete!"
