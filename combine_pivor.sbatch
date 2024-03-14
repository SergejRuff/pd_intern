#! /bin/bash
#BATCH --job-name=comcsv
#SBATCH --cpus-per-task=30
#SBATCH --time=48:00:00
#SBATCH --mem=1000G
#SBATCH --output=output/combine_pivor_%A_%a.out
#SBATCH --mail-type=ALL
#SBATCH --mail-user=Sergej.Ruff@stud.mh-hannover.de



echo "combine binary pivoted files into one"
echo ""

# Print job information
echo "Job ran on:" $(hostname)
echo "Start time:" $(date)

# Load modules
ml use ~/.local/easybuild/modules/all
ml load Python/3.10.4-GCCcore-11.3.0

python /hpc/scratch/project/ag-ixplain-cds/sergejruff/combine_pivot2.py



echo ""
echo "Job is done. End time:" $(date)

