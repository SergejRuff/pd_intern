#! /bin/bash
#BATCH --job-name=split
#SBATCH --cpus-per-task=125
#SBATCH --time=12:00:00
#SBATCH --mem=1000G
#SBATCH --output=output/split_%A_%a.out
#SBATCH --mail-type=ALL
#SBATCH --mail-user=Sergej.Ruff@stud.mh-hannover.de



echo "divide AG into A and G in seperate columns"
echo ""

# Print job information
echo "Job ran on:" $(hostname)
echo "Start time:" $(date)

# Load modules
ml use ~/.local/easybuild/modules/all
ml load Python/3.10.4-GCCcore-11.3.0

python /hpc/scratch/project/ag-ixplain-cds/sergejruff/split_labeled.py




echo ""
echo "Job is done. End time:" $(date)

