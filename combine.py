import os
import pandas as pd
import time
import argparse
import subprocess
import pyarrow
import sys


def transform_nucleotide(nucleotide):
    binary_dict = {'A': '00', 'C': '10', 'G': '01', 'T': '11'}
    binary_nucleotide = binary_dict.get(nucleotide, nucleotide)
    return binary_nucleotide


def combine_parquet_files(folder_path, output_file):
    # List all files in the folder
    files = os.listdir(folder_path)
    
    # Filter only .parquet files
    parquet_files = [file for file in files if file.endswith('.parquet')]
    
    # Initialize an empty list to store DataFrames
    dfs = []
    
    # Iterate through each .parquet file
    for file in parquet_files:
        # Read the parquet file
        df = pd.read_parquet(os.path.join(folder_path, file))
        df['CHRPOS'] = df['CHROM'] + '_' + df['POS'].astype(str)
        df = df.drop(["CHROM","POS","ALT","TYPE","GT","TGT"],axis=1)
        df['REF'] = df['REF'].apply(lambda x: ''.join(transform_nucleotide(n) for n in x)) 
        df['REF'] = df['REF'] *2

        # Append the DataFrame to the list
        dfs.append(df)
    
    # Concatenate all DataFrames into a single DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Save the combined DataFrame as parquet
    combined_df.to_parquet(output_file, index=False)

# Example usage:
folder_path = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/binary_2'
output_file = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/combined_2/combined.parquet'

combine_parquet_files(folder_path, output_file)

print("script is done")
