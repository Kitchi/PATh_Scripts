#! /usr/bin/env python

import os
from config import N_CHANNELS, SCRATCH_DIR, OUTPUT_DIR

n_channels = N_CHANNELS

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

with open('tclean_dag.dag', 'w') as fptr:
    for ii in range(n_channels):
        chan_name = f"chan{ii}"
        fptr.write(f"JOB tclean_{chan_name} tclean.sub\n")
        fptr.write(f"VARS tclean_{chan_name} channum=\"{ii}\" channame=\"{chan_name}\"\n")
        fptr.write(f"SCRIPT PRE tclean_{chan_name} split_channel.sh {ii} {chan_name}\n")
        fptr.write(f"SCRIPT POST tclean_{chan_name} cleanup.sh {ii} {chan_name}\n")
        fptr.write(f"\n")
