import os
import pandas as pd
import time
import argparse
import subprocess
import pyarrow
import sys

# Define the function to transform nucleotides to binary
#def transform_nucleotide(nucleotide):
#    binary_dict = {'A': '1000', 'C': '0100', 'G': '0010', 'T': '0001'}
#    binary_nucleotide = binary_dict.get(nucleotide, nucleotide)
#    return binary_nucleotide



def pivot_parquet_files(folder_path, output_file):
       df_p2 =pd.read_parquet(folder_path)

       # Create a new column 'CHRPOS' -> Rows in pivot dataframe
       #df_p2['CHRPOS'] = df_p2['CHROM'] + '_' + df_p2['POS'].astype(str)

       # Pivot the DataFrame -> Rows= Samples, Col=CHRPOS, values=binary
       pivot_df = df_p2.pivot_table(index='SAMPLE', columns='CHRPOS', values='binary', aggfunc='first') 

       # Apply the transformation to REF column
       #df_p2['REF'] = df_p2['REF'].apply(lambda x: ''.join(transform_nucleotide(n) for n in x))

       # double Ref to make sure it has the same format as mutations X/Y instead of just X.
       #df_p2['REF'] = df_p2['REF'] *2



       ## > add Ref to matrix, transpose it and fill NAs that way.
       # Drop all columns except "REF" and "CHROM"
       #df_p2_test = df_p2.loc[:, ['REF','CHRPOS']]
       # remove duplicates to make sure row number is teh same
       #df_p2_test = df_p2_test.drop_duplicates('CHRPOS')

       # make CHRPos row name -> that way we can map/merge it to the rows in pivot_df
       #df_p2_test.set_index('CHRPOS',inplace=True)

       # merge/ combine Ref column to pivot
       #df = pivot_df.T.merge(df_p2_test, how ="left", on ='CHRPOS')

       # fill missing data based on Ref column
       #df2 = df.T.fillna(df['REF'], axis=0).T

       # drop unwanted and unloved Ref Column
       #pivot_df = df2.drop("REF",axis=1).T

       # Save the combined DataFrame as parquet
       pivot_df.to_parquet(output_file, index=False)

       csv_output_file =os.path.splitext(output_file)[0] + '.csv'
       pivot_df.to_csv(csv_output_file,index=False)
# Example usage:
folder_path = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/combined_2/combined.parquet'
output_file = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/pivot_3/pivot.parquet'

pivot_parquet_files(folder_path, output_file)

print("script is done")

