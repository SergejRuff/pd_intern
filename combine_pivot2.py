import pandas as pd
import os
from multiprocessing import Pool, cpu_count
import gc  # Import the garbage collection module for memory management

# Function to read CSV file and return DataFrame
def read_csv(file):
    return pd.read_csv(file,index_col=0)

# Function to concatenate DataFrames row-wise with outer join
def concatenate_dfs_row_wise_outer_join(dfs):
    result = pd.DataFrame()  # Initialize an empty DataFrame
    for df in dfs:
        # Concatenate the current DataFrame with the result DataFrame row-wise with an outer join
        result = pd.concat([result, df], axis=1, ignore_index=False, sort=False)
        del df  # Delete the DataFrame to release memory
    return result

# Path to the folder containing CSV files
folder_path = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/pivot_3'

# Get list of CSV files in the folder
csv_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.csv')]

# Number of CPUs available for parallel processing
num_cpus = 30

# Create a pool of processes
with Pool(processes=num_cpus) as pool:
    # Read CSV files and concatenate them one by one
    partial_results = pool.map(read_csv, csv_files)
    joined_df = concatenate_dfs_row_wise_outer_join(partial_results)

    # Export the joined DataFrame as a CSV file
    output_folder = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/pivot_2'
    output_file_path = os.path.join(output_folder, 'joined_data3.csv')
    joined_df.to_csv(output_file_path, index=True)
    print(joined_df.head(5))
    print("Joined data exported as CSV to:", output_file_path)

    # Perform garbage collection to release memory
    del partial_results, joined_df
    gc.collect()
