import subprocess
import os
import pandas as pd
import time

# Record the start time
start_time = time.time()

# Set the input and output directories
input_directory = "/media/sergejruff/data/Data"
output_directory = '/media/sergejruff/My Book/Pakinson_data/output'

def check_bcftools():
    try:
        subprocess.run(["bcftools", "--version"], check=True)
        print("bcftools is installed.")
    except subprocess.CalledProcessError:
        print("Error: bcftools is not installed. Please install it before running this script.")
        exit(1)

# Call the function to check bcftools installation
check_bcftools()

# Check if the input directory exists
if not os.path.exists(input_directory):
    print(f"Error: Directory not found: {input_directory}")
    exit(1)

# Check if the output directory exists, if not, create it
#if not os.path.exists(output_directory):
#    os.makedirs(output_directory)
# this code is fine for local pcs. i dont want to creat folders on hpc which isnt password protected.

# Get a list of all files in the input directory that start with "LRRK2" and end with ".g.vcf.gz"
#files = [file for file in os.listdir(input_directory) if file.endswith(".g.vcf.gz")]
# to go only through the LRRK2 files ans ignore ST
files = [file for file in os.listdir(input_directory) if file.startswith("LRRK2") and file.endswith(".g.vcf.gz")][:2]

# Accumulate dataframes for each file
dfs = []

# Iterate through each file
for input_file in files:
    # Change to the input directory
    os.chdir(input_directory)

    print(f"Processing file: {input_file}")

    # Run bcftools command and capture the output
    bcftools_command = [
        "bcftools",
        "query",
        "-f[%CHROM;%POS;%SAMPLE;%REF;%ALT;%TYPE;%TGT\n]",
        "-iTYPE=\"SNP\"",
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

    # Append the dataframe to the list
    dfs.append(output_df)

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
