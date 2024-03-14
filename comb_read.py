import os
import pyarrow.parquet as pq
import pandas as pd

directory = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/combined_2'
file_name = 'combined.parquet'
file_path = os.path.join(directory, file_name)
output_file_path = os.path.join(directory, 'parser_output.txt')

# Open the Parquet file
parquet_file = pq.ParquetFile(file_path)

# Read the first 10 rows of the Parquet file
table = parquet_file.read_row_group(1)

df = table.to_pandas()

table = df.head(10)

# Redirecting print statements to a text file
with open(output_file_path, 'w') as output_file:
    # Print the first 10 rows
    print(f"First 10 rows of {file_name}:", file=output_file)
    print(table, file=output_file)

    unique_sample = df['SAMPLE'].unique()
    print(unique_sample, file=output_file)

    num_rows = parquet_file.num_row_groups
    print(num_rows, file=output_file)

    # Initialize a set to store unique SAMPLE values
    unique_samples = set()

    # Iterate over each row group
    for i in range(parquet_file.num_row_groups):
        # Read the row group
        table = parquet_file.read_row_group(i)

        # Convert the PyArrow Table to a pandas DataFrame
        df = table.to_pandas()

        # Add unique SAMPLE values from this row group to the set
        unique_samples.update(df['SAMPLE'].unique())

    # Print the number of unique SAMPLE values across all row groups
    print(f"Number of unique values of 'SAMPLE' across all row groups: {len(unique_samples)}", file=output_file)
