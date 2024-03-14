#! /bin/bash
#BATCH --job-name=combine
#SBATCH --cpus-per-task=1
#SBATCH --time=12:00:00
#SBATCH --mem=500G
#SBATCH --output=output/parser_%A_%a.out
#SBATCH --mail-type=ALL
#SBATCH --mail-user=Sergej.Ruff@stud.mh-hannover.de



echo "This script is transforming palindrom sequences like A/T and T/A making sure both are treated the same (both become A/T)"
echo ""

# Print job information
echo "Job ran on:" $(hostname)
echo "Start time:" $(date)

# Load modules
ml use ~/.local/easybuild/modules/all
ml load Python/3.10.4-GCCcore-11.3.0

python /hpc/scratch/project/ag-ixplain-cds/sergejruff/combine.py 
    


echo ""
echo "Job is done. End time:" $(date)

