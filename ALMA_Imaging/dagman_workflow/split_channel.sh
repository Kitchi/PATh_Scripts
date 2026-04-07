#! /bin/bash

# Get script directory and source config
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

CHANNUM=$1
CHANNAME=$2

apptainer exec ${CONTAINER_SIF} python3 ${SPLIT_SCRIPT} \
	--vis ${INPUT_MS} \
	--channum ${CHANNUM} \
	--outvis ${SCRATCH_DIR}/hltau_240k.0_chan${CHANNUM}.ms
