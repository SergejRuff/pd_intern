#! /bin/bash
#BATCH --job-name=pivot_3
#SBATCH --cpus-per-task=8
#SBATCH --time=24:00:00
#SBATCH --mem=600G
#SBATCH --output=output/pivot_%A_%a.out
#SBATCH --mail-type=ALL
#SBATCH --mail-user=Sergej.Ruff@stud.mh-hannover.de



echo "This script generates a new matrix with col=Position and Row= Samples"
echo ""

# Print job information
echo "Job ran on:" $(hostname)
echo "Start time:" $(date)

# Load modules
ml use ~/.local/easybuild/modules/all
ml load Python/3.10.4-GCCcore-11.3.0

python /hpc/scratch/project/ag-ixplain-cds/sergejruff/pivot.py



echo ""
echo "Job is done. End time:" $(date)


