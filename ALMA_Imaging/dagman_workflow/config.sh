#!/bin/bash
# Configuration file for ALMA DAGMAN imaging workflow (bash version)
# Centralized location for all paths and parameters

# Container paths
export CONTAINER_SIF='/path-facility/ap1/data/srikrishna.sekhar/containers/casa-6-ultraminimal-zstd.sif'

# Script paths
export SPLIT_SCRIPT='/home/srikrishna.sekhar/src/scripts/ALMA_Imaging/dagman_workflow/split_channel.py'

# Data paths
export INPUT_MS='/home/srikrishna.sekhar/data/data/raw/hltau_240k.0.ms'
export SCRATCH_DIR='/home/srikrishna.sekhar/data/data/channel_images_scratch'
export OUTPUT_DIR='/path-facility/data/srikrishna.sekhar/data/outputs/scaling_tests/HLTau/run_1'
