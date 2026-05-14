#! /bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=144
#SBATCH --mem=128GB
#SBATCH --job-name=tclean
#SBATCH --output=logs/tclean_%A_%a.out
#SBATCH --error=logs/tclean_%A_%a.err
#SBATCH --time=3:00:00
# Submit with: sbatch --array=0-N%10 tclean.sbatch
# Single node:  sbatch --array=0-0 tclean.sbatch  (or omit --array)

source /lustre/aoc/sciops/ssekhar/soft/py38/bin/activate

srun python tclean.py /lustre/aoc/projects/ngvla/archive_ms_v4/ALMA/2019.1.00592.S/ms_v2/uid___A002_Xf0fd41_X5f5a_target.ms \
    --imsize 4096 \
    --cell 0.004arcsec \
    --gridder mosaic \
    --stokes I \
    --usemask auto-multithresh \
    --niter 50000 \
    --threshold '2mJy' \
    --mem_per_chan 8.0 \
    --total_ram 128.0 \
    --total_cores 144
