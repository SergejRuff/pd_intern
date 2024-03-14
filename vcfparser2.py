import subprocess
import os
import pandas as pd
import time
from multiprocessing import Pool, cpu_count
import argparse

def check_bcftools():
    try:
        subprocess.run(["bcftools", "--version"], check=True)
        print("bcftools is installed.")
    except subprocess.CalledProcessError:
        print("Error: bcftools is not installed. Please install it before running this script.")
        exit(1)

# Call the function to check bcftools installation
check_bcftools()

# Parse command line arguments
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--threads', type=int, help='Number of threads to use')
args = parser.parse_args()

# Set the input and output directories
input_directory = "/hpc/scratch/project/ag-ixplain-cds/sergejruff"
output_directory = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output'

# Record the start time
start_time = time.time()

# Check if the input directory exists
if not os.path.exists(input_directory):
    print(input_directory)
    print(f"Error: Directory not found: {input_directory}")
    exit(1)

# Get a list of all files in the input directory that start with "LRRK2" and end with ".g.vcf.gz"
files = [file for file in os.listdir(input_directory) if file.startswith("LRRK2") and file.endswith(".g.vcf.gz")]

def process_file(input_file):
    try:
        # Change to the input directory
        os.chdir(input_directory)

        print(f"Processing file: {input_file}")

        # Run bcftools command and capture the output
        bcftools_command = [
            "bcftools",
            "query",
            "-f[%CHROM;%POS;%SAMPLE;%REF;%ALT;%TYPE;%GT;%TGT\n]",
            "-iTYPE=\"SNP\" && GT!=\"REF\"",
            input_file
        ]

        result = subprocess.run(bcftools_command, stdout=subprocess.PIPE, text=True, check=True)
        output = result.stdout.replace("<NON_REF>", "").replace(",", "_").replace(";", ",").replace("_", ";")

        # Split the output into rows using semicolon as the row separator
        rows = output.strip().split("\n")

        # Define the number of columns
        num_columns = 7

        # Split each row into values using comma as the column separator
        values = [row.split(",")[:num_columns] for row in rows]

        # Create a DataFrame from the list of values
        output_df = pd.DataFrame(values, columns=["CHROM", "POS", "SAMPLE", "REF", "ALT", "TYPE", "GT"])

        # Exclude rows with 'chrX' or 'chrY' in the 'CHROM' column
        output_df = output_df[~output_df['CHROM'].isin(['chrX', 'chrY'])]

        return output_df
    except Exception as e:
        print(f"Error processing file {input_file}: {e}")
        return None


# args.threads
# Use multiprocessing Pool to parallelize file processing
with Pool(processes=args.threads) as pool:
    dfs = pool.map(process_file, files)

# Concatenate all dataframes into one
final_df = pd.concat(dfs, ignore_index=True)

# Specify the output file path
output_file_path = os.path.join(output_directory, "combined_output.parquet")

# Save the combined DataFrame to a single Parquet file
final_df.to_parquet(output_file_path)

# Record the end time
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

# Print the overall elapsed time
print(f"Script completed in {elapsed_time:.2f} seconds. Combined output saved to {output_file_path}")

