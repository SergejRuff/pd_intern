#! /bin/bash
#BATCH --job-name=pivot_binary
#SBATCH --cpus-per-task=1
#SBATCH --time=08:00:00
#SBATCH --mem=200G
#SBATCH --output=output/pivorbinary_%A_%a.out
#SBATCH --array=0-362
#SBATCH --mail-type=ALL
#SBATCH --mail-user=Sergej.Ruff@stud.mh-hannover.de

echo "pivot multiple files for later merging into one big file."
echo ""

# Print job information
echo "Job ran on:" $(hostname)
echo "Start time:" $(date)

# Load modules
ml use ~/.local/easybuild/modules/all
ml load Python/3.10.4-GCCcore-11.3.0

# Specify the folder containing the parquet files
input_directory="/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/binary"

# Get the list of files ending with .g.vcf.gz
files=($(find "${input_directory}" -maxdepth 1 -type f -name '*.parquet'))


# Function to process each file
process_file() {
    local file="$1"
    echo "Processing file: $file"
    python /hpc/scratch/project/ag-ixplain-cds/sergejruff/pivot_2.py --input-file  "$file"
    echo "Finished processing file: $file"
}

# Iterate over the files in parallel
process_file "${files[SLURM_ARRAY_TASK_ID]}"



echo ""
echo "Job is done. End time:" $(date)

