# ALMA DAGMAN Imaging Workflow

Automated HTCondor DAGMan workflow for parallel ALMA channel imaging using CASA.

## Overview

This workflow processes 238,011 channels in parallel using HTCondor DAGMan. For each channel:
1. **PRE script**: Splits the channel from the main MS file
2. **JOB**: Runs CASA tclean on the channel
3. **POST script**: Cleans up intermediate files

## Quick Start

### 1. Configure paths

Edit `config.py` and `config.sh` to match your environment:

```bash
# Key paths to check:
- CONTAINER_SIF: Path to CASA container
- INPUT_MS: Path to input measurement set
- SCRATCH_DIR: Temporary storage for split channels
- OUTPUT_DIR: Final output location
```

### 2. Test with a small number of channels

```bash
./run_workflow.sh --test 10 --submit
```

### 3. Run full workflow

```bash
./run_workflow.sh --submit
```

## File Structure

```
dagman_workflow/
├── config.py              # Python configuration (paths, parameters)
├── config.sh              # Bash configuration (sourced by scripts)
├── generate_dagman.py     # Generates the DAG file
├── run_workflow.sh        # Main wrapper script (use this!)
├── tclean.sub             # HTCondor submit file for imaging jobs
├── tclean.py              # CASA tclean imaging script
├── split_channel.sh       # PRE script: splits channels
├── split_channel.py       # Python script called by split_channel.sh
├── cleanup.sh             # POST script: removes intermediate files
└── README.md              # This file
```

## Wrapper Script Usage

```bash
./run_workflow.sh [OPTIONS]

OPTIONS:
    -t, --test N         Test mode: only process N channels
    -s, --submit         Automatically submit DAG after generation
    -c, --check          Only check environment, don't generate DAG
    -h, --help           Show help message
```

### Examples

**Check environment only:**
```bash
./run_workflow.sh --check
```

**Test with 10 channels (don't submit):**
```bash
./run_workflow.sh --test 10
```

**Test with 10 channels and submit:**
```bash
./run_workflow.sh --test 10 --submit
```

**Generate full DAG (don't submit):**
```bash
./run_workflow.sh
```

**Generate and submit full DAG:**
```bash
./run_workflow.sh --submit
```

## Monitoring

### Check DAG status
```bash
condor_q -dag
```

### Monitor DAGMan log
```bash
tail -f tclean_dag.dag.dagman.out
```

### Check individual job logs
```bash
tail -f logs/tclean_*.log
```

### Remove DAG
```bash
condor_rm <cluster_id>
```

## Workflow Details

### Pre-Processing (split_channel.sh)
- Runs before each imaging job
- Splits a single channel from the main MS file
- Creates: `${SCRATCH_DIR}/hltau_240k.0_chan{N}.ms`

### Imaging Job (tclean.py via tclean.sub)
- Untars the input MS
- Runs CASA tclean with specified parameters
- Exports FITS image
- Creates tarball: `output.tar.gz`
- Transfers to: `${OUTPUT_DIR}/output_{N}.tar.gz`

### Post-Processing (cleanup.sh)
- Runs after each imaging job completes
- Removes intermediate split MS file
- Removes output tarball (if desired)

## Configuration Parameters

### Imaging Parameters (config.py)
- `GRIDDER`: mosaic
- `IMSIZE`: 2250 pixels
- `CELL`: 0.025arcsec
- `NITER`: 100000
- `THRESHOLD`: 2mJy
- `USEMASK`: user

### Resource Requests (config.py)
- `REQUEST_CPUS`: 1
- `REQUEST_MEMORY`: 8G
- `REQUEST_DISK`: 8G
- `MAX_RETRIES`: 10

## Troubleshooting

### Jobs held
Check held jobs:
```bash
condor_q -hold
```

View hold reason:
```bash
condor_q -af HoldReason
```

Release held jobs:
```bash
condor_release <cluster_id>
```

### Check job logs
```bash
# Standard output
cat logs/tclean_*.out

# Error output
cat logs/tclean_*.err

# HTCondor log
cat logs/tclean_*.log
```

### Regenerate DAG
```bash
rm tclean_dag.dag*
./run_workflow.sh
```

## Notes

- The workflow creates a `logs/` directory automatically
- PRE/POST scripts use absolute paths from config files
- Container runs via Apptainer/Singularity
- Output tarballs contain FITS images and timing information
- Periodic release enabled for held jobs (codes 11, 12)
- Jobs automatically vacate after 12 hours in queue or 7 hours running

## Cleanup

To completely clean up the workflow:

```bash
# Remove DAG and related files
rm tclean_dag.dag*

# Remove logs
rm -rf logs/

# Remove scratch files (be careful!)
# rm -rf ${SCRATCH_DIR}/*
```
