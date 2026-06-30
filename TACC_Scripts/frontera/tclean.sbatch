#! /bin/bash
#SBATCH --nodes=72
#SBATCH --partition=normal
#SBATCH --time=24:00:00
#SBATCH --output=logs/tclean_%j.out
#SBATCH --error=logs/tclean_%j.err

module load pylauncher

export OMP_NUM_THREADS=4
srun /work2/10747/ssekhar/frontera/soft/micromamba/envs/py312/bin/python tclean_pylauncher.py
