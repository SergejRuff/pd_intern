import os
import pandas as pd
import time
import argparse
import subprocess
import pyarrow
import sys




# Custom function to transform nucleotides to binary representation
#def transform_nucleotides(pair):
#    binary_dict = {'A': '1000', 'C': '0100', 'G': '0010', 'T': '0001'}
#    #binary_pair = ''.join([binary_dict[letter] for letter in pair.split('/')])
#    binary_pair = ''.join(binary_dict[letter] for letter in pair.split('/'))
#    return binary_pair



def transform_nucleotides(pair):
    binary_dict = {'A': '00', 'C': '10', 'G': '01', 'T': '11'}
    ref, alt = pair.split('/')
    binary_pair = binary_dict[ref] + binary_dict[alt]
    return binary_pair

# Parse command line arguments
parser = argparse.ArgumentParser(description='Process a single parquet  file.')
parser.add_argument('--input-file', type=str, help='Input Parquet file to process')
args = parser.parse_args()

# Set the output directory
output_directory = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/binary_2'

# Record the start time
start_time = time.time()

# Check if the input file exists
input_file = args.input_file
if not os.path.exists(input_file):
    print(f"Error: File not found: {input_file}")
    exit(1)

input_name =os.path.basename(args.input_file)
i_name = f"processing_log_{input_name}.txt"

###################################

# Open a text file to save the output of print statements
log_file_path = os.path.join(output_directory, i_name)
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

        alleles =df_copy['TGT'].str.split('/',expand=True).stack()
        allele_counts =alleles.value_counts()
        print(allele_counts)
 

        df_copy = df_copy[df_copy['TGT'].str.contains(r'^[CGTA]/[CGTA]$')]
        # Group by "CHROM" and "POS" and apply the function to each group
        #df_copy_reset = df_copy.reset_index()
        df_copy['binary'] = df_copy['TGT'].apply(lambda x: transform_nucleotides(x) if '/' in x else x)





        # Specify the output file path
        substring_to_remove = ".g.vcf.gz_output.parquet_sp.parquet"
        input_file_name = os.path.basename(input_file).replace(substring_to_remove,"") + "_binary.parquet"
        output_file_path = os.path.join(output_directory, input_file_name)

        # Save the modified DataFrame to a new Parquet file
        df_copy.to_parquet(output_file_path)

        print("new column:")
        print(df_copy.head())
 
        print("")
        print("count after removal of empty strings")  
        alleles =df_copy['TGT'].str.split('/',expand=True).stack()
        allele_counts =alleles.value_counts()
        print(allele_counts)


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

