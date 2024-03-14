import os
import pandas as pd
import time
import argparse
import subprocess
import pyarrow
import sys


unique_pairs = set()

def treat_palindromic_pairs(pair):
	values =pair.split('/')
	sorted_pair = '/'.join(sorted(values))
	unique_pairs.add(sorted_pair)
	return sorted_pair

# Parse command line arguments
parser = argparse.ArgumentParser(description='Process a single parquet  file.')
parser.add_argument('--input-file', type=str, help='Input Parquet file to process')
args = parser.parse_args()

# Set the output directory
output_directory = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/parquet_same_palindrom'

# Record the start time
start_time = time.time()

# Check if the input file exists
input_file = args.input_file
if not os.path.exists(input_file):
    print(f"Error: File not found: {input_file}")
    exit(1)


###################################

# Open a text file to save the output of print statements
log_file_path = os.path.join(output_directory, "processing_log.txt")
with open(log_file_path, "w") as log_file:
    # Redirect standard output to the log file
    original_stdout = sys.stdout
    sys.stdout = log_file

    try:
        print(f"Processing file: {input_file}")

        # Read the input Parquet file
        df = pd.read_parquet(input_file)

        # Make a copy of the DataFrame
        df_copy = df.copy()

        print("Before transformation:")
        print(df_copy["TGT"].value_counts())
        print("")

        

        # Group by "CHROM" and "POS" and apply the function to each group
        df_copy_reset = df_copy.reset_index()
        df_copy['TGT'] = df_copy_reset.groupby(["CHROM", "POS"])['TGT'].transform(lambda x: x.apply(treat_palindromic_pairs))

        
     

        # Specify the output file path
        output_file_path = os.path.join(output_directory, os.path.basename(input_file) + "_sp.parquet")

        # Save the modified DataFrame to a new Parquet file
        df_copy.to_parquet(output_file_path)

        print("After transformation:")
        print(df_copy["TGT"].value_counts())

        # Print completion message
        print(f"Processing completed. Output saved to: {output_file_path}")
    except Exception as e:
        print(f"Error processing file {input_file}: {e}")

    # Restore standard output
    sys.stdout = original_stdout


# Record the end time
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

# Print the overall elapsed time
print(f"Script completed in {elapsed_time:.2f} seconds.")


