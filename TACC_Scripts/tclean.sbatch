#! /bin/bash
#SBATCH --nodes=1
#SBATCH --partition=gg
#SBATCH --time=05:00:00
#SBATCH --output=logs/tclean_%j.out
#SBATCH --error=logs/tclean_%j.err

module load pylauncher
/home1/10747/ssekhar/work/src/spack/bin/spack  env activate --sh casa-dev
source ~/work/src/CASA/master/venv/bin/activate

export OMP_NUM_THREADS=4

srun python tclean_pylauncher.py
