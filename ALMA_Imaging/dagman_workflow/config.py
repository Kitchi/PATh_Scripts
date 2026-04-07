"""
Configuration file for ALMA DAGMAN imaging workflow
Centralized location for all paths and parameters
"""

# Number of channels to process
N_CHANNELS = 238011

# Container paths
CONTAINER_SIF = '/path-facility/ap1/data/srikrishna.sekhar/containers/casa-6-ultraminimal-zstd.sif'

# Script paths
SPLIT_SCRIPT = '/home/srikrishna.sekhar/src/scripts/ALMA_Imaging/dagman_workflow/split_channel.py'

# Data paths
INPUT_MS = '/home/srikrishna.sekhar/data/data/raw/hltau_240k.0.ms'
SCRATCH_DIR = '/home/srikrishna.sekhar/data/data/channel_images_scratch'
OUTPUT_DIR = '/path-facility/data/srikrishna.sekhar/data/outputs/scaling_tests/HLTau/run_1'

# Imaging parameters
GRIDDER = 'mosaic'
IMSIZE = 2250
CELL = '0.025arcsec'
STOKES = 'I'
NITER = 100000
USEMASK = 'user'
THRESHOLD = '2mJy'

# Resource requests
REQUEST_CPUS = 1
REQUEST_MEMORY = '8G'
REQUEST_DISK = '8G'
MAX_RETRIES = 10
MAX_IDLE = 10000
