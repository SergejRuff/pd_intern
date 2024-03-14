#! /bin/bash
#BATCH --job-name=dttreeGT
#SBATCH --cpus-per-task=1
#SBATCH --time=168:00:00
#SBATCH --mem=1300G
#SBATCH --output=output/GTtree_%A_%a.out
#SBATCH --mail-type=ALL
#SBATCH --mail-user=Sergej.Ruff@stud.mh-hannover.de



echo "tune hyperparameters for DT"
echo ""

# Print job information
echo "Job ran on:" $(hostname)
echo "Start time:" $(date)

# Load modules
ml use ~/.local/easybuild/modules/all
ml load Python/3.10.4-GCCcore-11.3.0

python /hpc/scratch/project/ag-ixplain-cds/sergejruff/hyperparametertuning.py





echo ""
echo "Job is done. End time:" $(date)


