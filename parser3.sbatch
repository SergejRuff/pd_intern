#!/bin/bash
#SBATCH --job-name=parser2
#SBATCH --cpus-per-task=1
#SBATCH --time=24:00:00
#SBATCH --mem=16G
#SBATCH --output=output/parser_%A_%a.out
#SBATCH --array=0-362

echo "This script is looking for missing data in the REF column of multiple vcf or gvcf files in a specified folder and exports the results as a txt file"
echo ""

# Print job information
echo "Job ran on:" $(hostname)
echo "Start time:" $(date)

# Load modules
ml use ~/.local/easybuild/modules/all
ml load BCFtools
ml load Python/3.10.4-GCCcore-11.3.0

# Specify the folder containing the .g.vcf.gz files
input_directory="/hpc/scratch/project/ag-ixplain-cds/sergejruff"

# Get the list of files ending with .g.vcf.gz
files=($(find "${input_directory}" -maxdepth 1 -type f -regex '.*\.g\.vcf\.gz$'))


# Function to process each file
#process_file() {
#    local file="$1"
#    echo "Processing file: $file"
#    python /hpc/scratch/project/ag-ixplain-cds/sergejruff/vcfparser3.py --threads 1 "$files"
#    echo "Finished processing file: $file"
#}

# Iterate over the files in parallel
#for ((i = SLURM_ARRAY_TASK_ID; i <= ${#files[@]}; i += SLURM_ARRAY_TASK_COUNT)); do
#    process_file "${files[i - 1]}" &
#
#done
#wait

#echo "processing files $files[SLURM_ARRAY_TASK_ID]"
#python /hpc/scratch/project/ag-ixplain-cds/sergejruff/vcfparser4.py --threads 1 "${files[SLURM_ARRAY_TASK_ID]}"

# Function to process each file
process_file() {
    local file="$1"
    echo "Processing file: $file"
    python /hpc/scratch/project/ag-ixplain-cds/sergejruff/vcfparser4.py --input-file  "$file"
    echo "Finished processing file: $file"
}

# Iterate over the files in parallel
process_file "${files[SLURM_ARRAY_TASK_ID]}"



echo ""
echo "Job is done. End time:" $(date)
