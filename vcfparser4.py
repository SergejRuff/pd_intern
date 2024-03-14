import os
import pandas as pd
import time
import argparse
import subprocess
import pyarrow

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
parser = argparse.ArgumentParser(description='Process a single .g.vcf.gz file.')
parser.add_argument('--input-file', type=str, help='Input .g.vcf.gz file to process')
args = parser.parse_args()

# Set the output directory
output_directory = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/parquet'

# Record the start time
start_time = time.time()

# Check if the input file exists
input_file = args.input_file
if not os.path.exists(input_file):
    print(f"Error: File not found: {input_file}")
    exit(1)

try:
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
    num_columns = 8

    # Split each row into values using comma as the column separator
    values = [row.split(",")[:num_columns] for row in rows]

    # Create a DataFrame from the list of values
    output_df = pd.DataFrame(values, columns=["CHROM", "POS", "SAMPLE", "REF", "ALT", "TYPE", "GT","TGT"])

    # Exclude rows with 'chrX' or 'chrY' in the 'CHROM' column
    output_df = output_df[~output_df['CHROM'].isin(['chrX', 'chrY'])]

    # Specify the output file path
    output_file_path = os.path.join(output_directory, os.path.basename(input_file) + "_output.parquet")

    # Save the DataFrame to a Parquet file
    output_df.to_parquet(output_file_path)

    # Print completion message
    print(f"Processing completed. Output saved to: {output_file_path}")
except Exception as e:
    print(f"Error processing file {input_file}: {e}")

# Record the end time
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

# Print the overall elapsed time
print(f"Script completed in {elapsed_time:.2f} seconds.")

