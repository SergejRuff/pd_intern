import pandas as pd
import pyarrow
import multiprocessing
import numpy as np

def nucleotide_to_binary(nucleotide):
    nucleotide_dict = {'A': '1000', 'C': '0100', 'G': '0010', 'T': '0001'}
    return nucleotide_dict.get(nucleotide, '0000')



#def process_chunk(chunk):
#    chunk = chunk.T
#    encoded_chunks = []
#    for column in chunk.columns:
#        encoded_chunk = pd.DataFrame(index=chunk.index)
#        binary_representation = chunk[column].apply(lambda x: ''.join([nucleotide_to_binary(n) for n in x]))
#        for i in range(8):
#            encoded_chunk[f'{column}_{i+1}'] = binary_representation.apply(lambda x: int(x[i]))
#        encoded_chunks.append(encoded_chunk)
#    # concatenated_chunk = pd.concat(encoded_chunks.values(), axis=1)
#    #concatenated_chunk['ageonset'] = ageonset_row
#    return encoded_chunks


#def process_chunk(chunk):
#    chunk = chunk.T
#    concatenated_chunks = pd.DataFrame(index=chunk.index)
#    for column in chunk.columns:
#        binary_representation = chunk[column].apply(lambda x: ''.join([nucleotide_to_binary(n) for n in x]))
#        for i in range(8):
#            concatenated_chunks[f'{column}_{i+1}'] = binary_representation.apply(lambda x: int(x[i]))
#    return concatenated_chunks


def process_chunk(chunk):
    processed_columns = []
    for column in chunk.columns:
        processed_columns.append(chunk[column].apply(lambda x: ''.join([nucleotide_to_binary(n) for n in x])))
    return pd.concat(processed_columns, axis=1)

def main():
    df = pd.read_parquet('/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/labed_data/ldata.parquet')
    print(df.tail(10))
    export_parquet = "/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/labed_data/string8_data.parquet"
    export_csv = "/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/labed_data/string8_data.csv"
    ageonset_row = df.loc["ageonset"]
    df  = df.drop("ageonset",axis=0)
    print("after dropping label")
    print("")
    print(df.tail(10))

    num_processes = 125
    chunks = np.array_split(df, num_processes)

    with multiprocessing.Pool(processes=num_processes) as pool:
        processed_chunks = pool.map(process_chunk,chunks)
    
    print("first chunk")
    print("")
    print(processed_chunks[0])
    final_df = pd.concat(processed_chunks, axis=0)
    #final_df =final_df.T
    #final_df = pd.concat([final_df, ageonset_row])
    final_df.loc["ageonset"] = ageonset_row 
    print(final_df.tail(10))

    final_df.to_parquet(export_parquet, index=True)
    final_df.to_csv(export_csv, index=True)
# Call the main function directl
main()
