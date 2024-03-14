#!/bin/bash
#SBATCH --job-name=missing_data_detector
#SBATCH --cpus-per-task=10
#SBATCH --time=24:00:00
#SBATCH --ntasks=10
#SBATCH --mem=64G
#SBATCH --output=output/missing_data_detector_%j.txt
#SBATCH --partition=leinegpu_long # only for GPU jobs
#SBATCH --gres=gpu:1 # only for GPU jobs
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

# Specify the full path to your bash script
bash /hpc/scratch/project/ag-ixplain-cds/sergejruff/missing_data_detector.sh

# Wait for the background jobs to finish
wait

echo ""
echo "Job is done. End time:" $(date)
