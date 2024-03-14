#! /bin/bash
#BATCH --job-name=fillb
#SBATCH --cpus-per-task=125
#SBATCH --time=24:00:00
#SBATCH --mem=1000G
#SBATCH --output=output/flb_%A_%a.out
#SBATCH --mail-type=ALL
#SBATCH --mail-user=Sergej.Ruff@stud.mh-hannover.de



echo "fill nas in pivot table with ref and add labels"
echo ""

# Print job information
echo "Job ran on:" $(hostname)
echo "Start time:" $(date)

# Load modules
ml use ~/.local/easybuild/modules/all
ml load Python/3.10.4-GCCcore-11.3.0

python /hpc/scratch/project/ag-ixplain-cds/sergejruff/fill_label2.py



echo ""
echo "Job is done. End time:" $(date)


