#! /bin/bash

# Get script directory and source config
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

CHANNUM=$1
CHANNAME=$2

rm -rf ${SCRATCH_DIR}/hltau_240k.0_chan${CHANNUM}.ms
rm -rf ${OUTPUT_DIR}/output_${CHANNUM}.tar.gz
