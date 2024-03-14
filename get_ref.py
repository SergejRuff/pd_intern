import pandas as pd
import pyarrow.parquet as pq
import os
import pyarrow

# Directory containing the Parquet files
input_folder = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/binary/'
output_folder = '/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/'

# Initialize an empty DataFrame to store combined data
combined_df = pd.DataFrame(columns=['REF', 'CHRPOS'])

# Iterate through each file in the folder
for file_name in os.listdir(input_folder):
    if file_name.endswith('.parquet'):
        # Read the Parquet file
        file_path = os.path.join(input_folder, file_name)
        df = pq.read_table(file_path).to_pandas()
        df['CHRPOS'] = df['CHROM'] + '_' + df['POS'].astype(str)        
        # Double every string value in the 'REF' column
        df['REF'] = df['REF'] * 2
        
        # Keep only the 'REF' and 'CHRPOS' columns
        df = df[['REF', 'CHRPOS']]
        
        # Combine the data row-wise
        combined_df = pd.concat([combined_df, df], ignore_index=True)

# Drop duplicates based on 'CHRPOS'
combined_df.drop_duplicates(subset='CHRPOS', inplace=True)

# Save the combined DataFrame to CSV and Parquet files
combined_csv_path = os.path.join(output_folder, 'combined_data.csv')
combined_df.to_csv(combined_csv_path, index=False)

combined_parquet_path = os.path.join(output_folder, 'combined_data.parquet')
combined_df.to_parquet(combined_parquet_path)

# Display the resulting DataFrame
print(combined_df)

