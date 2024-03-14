import pandas as pd
import pyarrow
import multiprocessing
import numpy as np

def split_binary_string(string):
    return [int(char) for char in string]

def process_chunk(chunk):
    df = chunk
    
    # Applying the split function to each cell of the DataFrame
    df_split = df.map(split_binary_string).explode(list(df.columns))


    # Create a NumPy array with values 1 through 8 repeated as needed
    counter_values = np.tile(np.arange(1, 9), len(df_split.index) // 8 + 1)[:len(df_split.index)]

    # Append the counter values to the index
    df_split.index = df_split.index.astype(str) + '_' + counter_values.astype(str)
    return df_split

def main():
    df = pd.read_parquet('/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/labed_data/string8_data.parquet')
    df.loc['ageonset'] = df.loc['ageonset'].map({'early-onset': 0, 'late-onset': 1})
    print(df.tail(10))
    export_parquet = "/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/labed_data/string8_split.parquet"
    export_csv = "/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/labed_data/string8_split.csv"
    ageonset_row = df.loc["ageonset"]
    df  = df.drop("ageonset",axis=0)
    print("after dropping label")
    print("")
    print(df.tail(10))
    # Assuming df is your DataFrame
    # For demonstration, let's say the first cell is at row index 0 and column index 0
    first_cell = df.iloc[0, 0]

    # Check the type of the first cell
    cell_type = type(first_cell)
    print("Type of the first cell:", cell_type)
    num_processes = 125
    chunks = np.array_split(df, num_processes)

    with multiprocessing.Pool(processes=num_processes) as pool:
        processed_chunks = pool.map(process_chunk,chunks)

    print("first chunk")
    print("")
    print(processed_chunks[0].head(20))
    final_df = pd.concat(processed_chunks, axis=0)
    #final_df =final_df.T
    #final_df = pd.concat([final_df, ageonset_row])
    final_df.loc["ageonset"] = ageonset_row
    print(final_df.tail(10))

    final_df.to_parquet(export_parquet, index=True)
    final_df.to_csv(export_csv, index=True)
# Call the main function directl
main()

