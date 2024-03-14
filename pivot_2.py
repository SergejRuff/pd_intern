import os
import argparse
import pandas as pd
import pyarrow

def pivot_parquet_files(input_file, output_file):
    df = pd.read_parquet(input_file)
    df['CHRPOS'] = df['CHROM'] + '_' + df['POS'].astype(str)
    df['TGT'] = df['TGT'].str.replace('/', '')
    pivot_df = df.pivot_table(index='CHRPOS', columns='SAMPLE', values='TGT', aggfunc='first')
    pivot_df.to_parquet(output_file)  # Specify the output file path here
    csv_output_file = os.path.splitext(output_file)[0] + '.csv'
    pivot_df.to_csv(csv_output_file, index=True)

# Parse command line arguments
parser = argparse.ArgumentParser(description='Process a single parquet file.')
parser.add_argument('--input-file', type=str, help='Input Parquet file to process')
args = parser.parse_args()

input_file = args.input_file

# Define output file path based on input file name
output_file = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/pivot_3/pivot_' + os.path.splitext(os.path.basename(input_file))[0] + '.parquet'

# Process the input file
pivot_parquet_files(input_file, output_file)

print("Script is done.")
