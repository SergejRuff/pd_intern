import pyarrow.parquet as pq
import pyarrow as pa

print("Pyarrow version:", pa.__version__)
default_max_memory_size = pa.ipc.get_max_memory_size()
print("Default maximum memory size for PyArrow:", default_max_memory_size)
def check_parquet_file(filename):
    try:
        # Open the Parquet file
        parquet_file = pq.ParquetFile(filename)
        
        # Attempt to read some metadata or data from the file
        # This will raise an error if the file is corrupted
        metadata = parquet_file.metadata
        columns = parquet_file.schema.names
        
        # You can add more checks as needed
        
        print(f"The Parquet file '{filename}' seems to be valid.")
    except Exception as e:
        print(f"The Parquet file '{filename}' appears to be corrupted: {e}")

# Replace '/path/to/your/file.parquet' with the actual path to your Parquet file
check_parquet_file('/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/pivot_3/pivot_LRRK2AJ2610001_binary.parquet')

