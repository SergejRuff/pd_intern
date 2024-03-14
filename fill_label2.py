import pandas as pd
from multiprocessing import Pool, cpu_count
import numpy as np
import pyarrow

export_csv = "/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/labed_data/ldata.csv"
export_parquet = "/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/labed_data/ldata.parquet"

# import data
ref_df = pd.read_csv("/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/combined_data.csv")
combined_df = pd.read_csv("/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/pivot_2/joined_data3.csv",index_col=0)
csv_df = pd.read_csv("/hpc/scratch/project/ag-ixplain-cds/sergejruff/label.csv")

ref_df.set_index('CHRPOS', inplace=True)

# Define a function to merge, fill NaNs, and drop columns
def merge_fill_drop(df_chunk):
    # Merge dataframes
    #merged_df = df_chunk.merge(ref_df, how="left", left_index=True, right_index=True)
    # Fill NaNs and drop columns
    filled_df = df_chunk.T.fillna(df_chunk['REF'], axis=0).T.drop("REF", axis=1)
    return filled_df

# Define a function to add labels
def add_labels(partition):
    return pd.concat([partition, csv_transposed])

print(ref_df.head(5))
print(combined_df.head(5))
# Number of CPUs to use for parallel processing
num_cpus = 125  # Utilize all available CPUs

combined_df = combined_df.merge(ref_df, how="left", left_index=True, right_index=True)
print(combined_df.head(5))
# Split the dataframe into chunks for parallel processing
df_chunks = np.array_split(combined_df, num_cpus)

# Use multiprocessing Pool for parallel execution
with Pool(num_cpus) as pool:
    processed_chunks = pool.map(merge_fill_drop, df_chunks)

# Concatenate processed chunks
df_processed = pd.concat(processed_chunks)
print(df_processed.head(5))
# Transpose the CSV DataFrame and set the first column as index
csv_transposed = csv_df.set_index('lrrkid').T

# Use multiprocessing Pool for adding labels
with Pool(num_cpus) as pool:
    results = pool.map(add_labels, [df_processed])

# Concatenate results
final_df = pd.concat(results)
#final_df = df_processed
# Export data
final_df.to_csv(export_csv, index=True, header=True)
final_df.to_parquet(export_parquet,index=True)
