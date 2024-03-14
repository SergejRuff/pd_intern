#!/bin/bash
#SBATCH --job-name=parser
#SBATCH --cpus-per-task=123
#SBATCH --time=24:00:00
#SBATCH --mem=64G
#SBATCH --output=output/parser1_%j.txt
#SBATCH --mail-type=ALL
#SBATCH --mail-user=Sergej.Ruff@stud.mh-hannover.de

echo "This script is looking for missing data in the REF column of multiple vcf or gvcf-files in a specified folder and exports the results as a txt-file"
echo ""

# Print job information
echo "Job ran on:" $(hostname)
echo "Start time:" $(date)

# Load modules
ml use ~/.local/easybuild/modules/all
ml load BCFtools
ml load Python/3.10.4-GCCcore-11.3.0

# Specify the full path to your bash script
python /hpc/scratch/project/ag-ixplain-cds/sergejruff/mcpuparser1.py 

# Wait for the background jobs to finish
wait

echo ""
echo "Job is done. End time:" $(date)

