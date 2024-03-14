#!/bin/bash
#SBATCH --job-name=look
#SBATCH --cpus-per-task=8
#SBATCH --time=08:00:00
#SBATCH --mem=64G
#SBATCH --output=output/look1_%j.txt
#SBATCH --mail-type=ALL
#SBATCH --mail-user=Sergej.Ruff@stud.mh-hannover.de

echo "This script is inspecting the combined-file"
echo ""

# Print job information
echo "Job ran on:" $(hostname)
echo "Start time:" $(date)

# Load modules
ml use ~/.local/easybuild/modules/all
ml load Python/3.10.4-GCCcore-11.3.0

# Specify the full path to your bash script
python /hpc/scratch/project/ag-ixplain-cds/sergejruff/comb_read.py


# Wait for the background jobs to finish
wait

echo ""
echo "Job is done. End time:" $(date)

