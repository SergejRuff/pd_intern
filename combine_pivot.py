import pandas as pd
import os
from multiprocessing import Pool, cpu_count
import gc  # Import the garbage collection module for memory management
import pyarrow

# Function to read Parquet file and return DataFrame
def read_parquet(file):
    return pd.read_parquet(file)

# Function to concatenate DataFrames row-wise with outer join
def concatenate_dfs_row_wise_outer_join(dfs):
    result = pd.DataFrame()  # Initialize an empty DataFrame
    for df in dfs:
        # Concatenate the current DataFrame with the result DataFrame row-wise with an outer join
        result = pd.concat([result, df], axis=1, ignore_index=False, join='outer', sort=False)
        del df  # Delete the DataFrame to release memory
    return result

# Path to the folder containing Parquet files
folder_path = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/pivot_3'

# Get list of Parquet files in the folder
parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]

# Number of CPUs available for parallel processing
num_cpus = 30

# Create a pool of processes
with Pool(processes=num_cpus) as pool:
    # Read Parquet files and concatenate them one by one
    partial_results = pool.map(read_parquet, parquet_files)
    joined_df = concatenate_dfs_row_wise_outer_join(partial_results)

    # Export the joined DataFrame as a Parquet file
    output_folder = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/pivot_2'
    output_file_path = os.path.join(output_folder, 'joined_data.parquet')
    joined_df.to_parquet(output_file_path, index=False)
    print(joined_df.head(5))
    print("Joined data exported as Parquet to:", output_file_path)

    # Perform garbage collection to release memory
    del partial_results, joined_df
    gc.collect()
