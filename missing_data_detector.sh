#!/bin/bash

echo "this script is looking for missing data in the REF column of multiple vcf or gvcf-files in a specifies folder and exports the results as a txt-file"
echo ""

# Script: missing_data_detector2.sh
# Description: this script is looking for missing data in the REF column of multiple vcf or gvcf-files in a specifies folder and exports the results as a txt-file.
# Author: Sergej Ruff
# Date: January 26, 2024

# Usage: ./missing_data_detectotor.sh <fasta_ref> <input_directory> <output_directory>
# This script works on all vcf.gz files inside the specified directory. The output will be saved to the specified directory.
# generates only one txt file for every vcf-file inside the folder.
# Warning: >ou do need a copy of the human genome hg38 in fasta-format: https://github.com/broadinstitute/gatk/blob/master/src/test/resources/large/Homo_sapiens_assembly38.fasta.gz

# example: ./missing_data_detector.sh /media/sergejruff/My\ Book/Pakinson_data/data/Homo_sapiens_assembly38.fasta /media/sergejruff/data/Data /media/sergejruff/My\ Book/Pakinson_data/output/missingdata

ml use ~/.local/easybuild/modules/all
ml load BCFtools


# Function to check if bcftools is installed
function check_bcftools {
    if command -v bcftools &> /dev/null; then
        echo "bcftools is installed."
    else
        echo "Error: bcftools is not installed. Please install it before running this script. Use sudo apt-get install bcftools  "
        exit 1
    fi
}

# Call the function
check_bcftools

# Check if the correct number of arguments is provided
#if [ "$#" -ne 3 ]; then
#    echo "Usage: $0 <fasta_ref> <input_directory> <output_directory>"
#    exit 1
#fi

# Assign command-line arguments to variables
fasta_ref="/hpc/scratch/project/ag-ixplain-cds/sergejruff/fasta/Homo_sapiens_assembly38.fasta"
input_directory="/hpc/scratch/project/ag-ixplain-cds/sergejruff/"
output_directory="/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/"
output_file="$output_directory/missing_data.txt"



# Check if the directory exists
if [ ! -d "$input_directory" ]; then
    echo "Error: Directory not found: $input_directory"
    exit 1
fi




# Check if the output directory already exists
if [ ! -d "$output_directory" ]; then
    # Create the output directory if it doesn't exist
    mkdir -p "$output_directory"
fi



#Iterate over all .g.vcf.gz files in the input directory
#for gvcf_file in "$input_directory"/*.g.vcf.gz; do



#    echo "Processing file: $gvcf_file"



    # Append the output line to missing_data.txt
#    echo "$gvcf_file" >> "$output_file"
#    bcftools norm -f "$fasta_ref" --check-ref w "$gvcf_file" -o /dev/null 2>&1 | grep 'total/split/realigned/skipped:' >> "$output_file"




#done


#echo "Processing complete. Results saved to: $output_file"



# Get a list of files
files=("$input_directory"/*.g.vcf.gz)
#files=("${files[@]:0:10}")

# Function to process a single file
process_file() {
    local gvcf_file="$1"
    echo "Processing file: $gvcf_file"
    bcftools norm -f "$fasta_ref" --check-ref w "$gvcf_file" -o /dev/null 2>&1 | grep 'total/split/realigned/skipped:' >> "$output_file"
}

# Parallel processing using a basic loop
parallel_jobs=10
for gvcf_file in "${files[@]}"; do
    (process_file "$gvcf_file") &
    if ((++count % parallel_jobs == 0)); then
        wait
    fi
done
wait

echo "Processing complete. Results saved to: $output_file"
