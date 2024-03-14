
import pandas as pd


export_csv = "/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/labed_data/labeled_data.csv"
export_parquet = "/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/labed_data/labeled_data.parquet"


# import data
ref_df = pd.read_csv("/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/combined_data.csv")
combined_df = pd.read_parquet("/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/pivot_2/joined_data.parquet")
csv_df = pd.read_csv("/hpc/scratch/project/ag-ixplain-cds/sergejruff/label.csv")


ref_df.set_index('CHRPOS',inplace=True)


#df = combined_df.merge(ref_df , how ="left", on ='CHRPOS')
df = combined_df.merge(ref_df, how="left", left_index=True, right_index=True)

df2 = df.T.fillna(df['REF'], axis=0).T

pivot_df = df2.drop("REF",axis=1)

### add labels
# Transpose the CSV DataFrame and set the first column as index
csv_transposed = csv_df.set_index('lrrkid').T

# Add the transposed DataFrame to the original DataFrame
df = pd.concat([pivot_df, csv_transposed])

pivot_df.to_csv(export_csv,index=True,header=True)
pivot_df.to_parquet(export_parquet)

