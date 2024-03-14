import os
import pandas as pd
import pyarrow

directory = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/combined'
# '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/parquet_same_palindrom' for data where palindroms are treated the same


files = os.listdir(directory)

######################
## for all files #####
######################

#parquet_files = [file for file in files if file.endswith('.parquet')]

#parquet_files =parquet_files[:20]

#########################
# look at specific file #
#########################

# Provide the start prefix
#start_prefix = "combined"

# List files in the directory ending with '.parquet' and starting with the provided prefix
#parquet_files = [file for file in files  if file.startswith(start_prefix) and file.endswith('.parquet')]


#########################
# main part #############
#########################

#for file in parquet_files:
#
#	file_path = os.path.join(directory,file)
#	table= pd.read_parquet(file_path).head(10)
#	print(f"first 10 rows of {file}:")
#	print(table.head(10))
#	print("="*20)

file_path = os.path.join(directory,'combined.parquet')
table= pd.read_parquet(file_path).head(10)
print("first 10 rows")
print(table.head(10))

