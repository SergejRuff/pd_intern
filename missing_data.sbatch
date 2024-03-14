#!/bin/bash
#SBATCH --job-name=missing_data_detector
#SBATCH --cpus-per-task=10
#SBATCH --time=24:00:00
#SBATCH --ntasks=10
#SBATCH --mem=64G
#SBATCH --output=sbatch_output/-%x_-%j.txt # %j (job_id) %x(job_name)
#SBATCH --partition=leinegpu_long # only for GPU jobs
#SBATCH --gres=gpu:1 # only for GPU jobs
#SBATCH --mail-type=ALL
#SBATCH --mail-user=Sergej.Ruff@stud.mh-hannover.de

echo "this script is looking for missing data in the REF column of multiple vcf or gvcf-files in a specifies folder and exports the results as a txt-file"
echo ""

# node the job ran on + empty line
echo "Job ran on:" $(hostname)
echo ""


ml use ~/.local/easybuild/modules/all
ml load BCFtools


cd /hpc/scratch/project/ag-ixplain-cds/sergejruff

bash missing_data_detector.sh

wait 

echo ""
echo "job is done"