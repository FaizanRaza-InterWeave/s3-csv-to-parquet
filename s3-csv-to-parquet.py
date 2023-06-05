# This Python script is used for converting CSV files stored in AWS S3 into Parquet files. 
# The script first reads CSV files from the specified directories, converts the CSV data 
# into a Pandas DataFrame, and then writes the DataFrame as a Parquet file back to S3.
# The script operates in a chunked manner, meaning it processes a set amount of data at a time, 
# which is beneficial for large files that cannot be loaded into memory all at once. 
# The script uses the PyArrow library for writing data in the Parquet format, which is a 
# columnar storage file format optimized for use with big data processing frameworks.

# Import necessary libraries
from transform_s3_csv_to_parquet_config import *
import s3fs
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import sys

# Initialize S3 file system
s3 = s3fs.S3FileSystem(anon=False)

# Define data types and column names for CSV data
column_data_types = {"date": int, "type": str, "price": float, "amount": float}
column_names = ["date", "type", "price", "amount"]

# Function to process a set amount of data (a "chunk") and save it as a Parquet file
def process_and_save_as_parquet(work_directory, data_to_process, partition_index):
    # Helper function to create a filename for a Parquet file
    def create_filename(keys):
        return "_".join(["kaiko-order_book"] + list(keys) + ["part_{i}_" + str(partition_index).zfill(3)]) + ".parquet"
    
    # Convert date column from integer to datetime format
    data_to_process["date"] = pd.to_datetime(data_to_process["date"], unit="ms")
    
    # Add additional columns to the DataFrame
    data_to_process = data_to_process.assign(
        exchange=exchange.replace(" ", ""),
        symbol=symbol,
        year=year
    ).reset_index(drop=True)
    
    # Write the DataFrame as a Parquet file
    pq.write_to_dataset(
        pa.Table.from_pandas(data_to_process),
        root_path=outdir,
        partition_cols=["exchange", "symbol", "year"],
        basename_template=create_filename([exchange.replace(" ", ""), symbol, year]),
        filesystem=s3,
        coerce_timestamps="us",
        use_deprecated_int96_timestamps=True,
        compression="ZSTD",
        compression_level=19
    )

# Function to process CSV directories based on the provided year
def process_csv_directories(year):
    # Check the year value and return the directories to be processed
    if year not in ["ALL"] + [str(Y) for Y in range(2000, 2101)]:
        print(f"{year} not 'ALL' or a YYYY value")
        return

    # Get a list of directories to process
    directories_to_process = sorted(set(['/'.join(directory.split('/')[2:5]).split('_')[0] for directory in s3.glob(f"{indir}/*/*/{year}_*")])) if year != "ALL" else sorted(set(['/'.join(directory.split('/')[2:5]).split('_')[0] for directory in s3.glob(f"{indir}/*/*/*")]))
    print(f"got {len(directories_to_process)} to process for {year} year(s)")

    # Process each directory
    for directory in directories_to_process:
        process_directory(directory)

# Function to process a single directory
def process_directory(directory):
    # Split the directory string into individual parts
    exchange, symbol, year = directory.split("/")

    # Get a list of CSV files in the directory
    csv_files = sorted(["s3://" + file for file in s3.glob(indir + "/" + directory + "*/*")])
    if not csv_files:
        print('skip, no data')
        return

    # Initialize an empty DataFrame to store CSV data
    data_to_process = pd.DataFrame()
    partition_index = 1

    # Process each CSV file in a chunked manner
    for csv_file in csv_files:
        for i, data_chunk in enumerate(
            pd.read_csv(
                csv_file,
                index_col=False,
                dtype=column_data_types,
                usecols=column_names,
                float_precision="high",
                chunksize=chunksize,
                low_memory=False,
            )
        ):
            # Append the chunk to the DataFrame
            data_to_process = pd.concat([data_to_process, data_chunk])
            
            # If the DataFrame is large enough, process it and save it as a Parquet file
            if len(data_to_process) >= filesize:
                process_and_save_as_parquet(directory, data_to_process, partition_index)
                data_to_process = pd.DataFrame()
                partition_index += 1

    # If there is any remaining data in the DataFrame, process it and save it as a Parquet file
    if len(data_to_process) > 0:
        process_and_save_as_parquet(directory, data_to_process, partition_index)

    print(f"Processed: {exchange}, {symbol}, {year}")

# Call function to process directories for a specific year
process_csv_directories(year_to_process)