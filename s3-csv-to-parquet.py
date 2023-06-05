from transform_s3_csv_to_parquet_config import *
import s3fs
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import sys

s3 = s3fs.S3FileSystem(anon=False)

if year_to_process in ["ALL"] + [str(Y) for Y in range(2000, 2101)]:
    if year_to_process == "ALL":
        csv_dirs = sorted(set(['/'.join(d.split('/')[2:5]).split('_')[0] for d in s3.glob(f"{indir}/*/*/*")]))
    else:
        csv_dirs = sorted(set(['/'.join(d.split('/')[2:5]).split('_')[0] for d in s3.glob(f"{indir}/*/*/{year_to_process}_*")]))
    print(f"got {len(csv_dirs)} to process for {year_to_process} year(s)")
else:
    print(f"{year_to_process} not 'ALL' or a YYYY value")

dtype = {
    "date": int,
    "type": str,
    "price": float,
    "amount": float,
}
usecols = [0, 1, 2, 3]
names = ["date", "type", "price", "amount"]


def do_work(work_dir, work_data, p):
    def my_filename(keys):
        return (
            "_".join(["kaiko-order_book"] + list(keys) + ["part_{i}_" + str(p).zfill(3)])
            + ".parquet"
        )
    work_data["date"] = pd.to_datetime(work_data["date"], unit="ms")
    work_data = work_data.assign(
        **{"exchange": exchange.replace(" ", ""), "symbol": symbol, "year": year}
    ).reset_index(drop=True)
    pq.write_to_dataset(
        pa.Table.from_pandas(work_data),
        root_path=outdir,
        partition_cols=["exchange", "symbol", "year"],
        basename_template=my_filename([exchange.replace(" ", ""), symbol, year]),
        filesystem=s3,
        coerce_timestamps="us",
        use_deprecated_int96_timestamps=True,
        compression="ZSTD",
        compression_level=19
    )
    return


for work_dir in csv_dirs:
    exchange, symbol, year = work_dir.split("/")
    print(f"{[exchange, symbol, year]} :: ", end="")
    work_csvs = sorted(["s3://" + d for d in s3.glob(indir + "/" + work_dir + "*/*")])
    if len(work_csvs) < 0:
        print('skip, no data')
        continue
    work_data = pd.DataFrame()
    p = 1
    for work_csv in work_csvs:
        for i, chunk in enumerate(
            pd.read_csv(
                work_csv,
                index_col=False,
                dtype=dtype,
                usecols=usecols,
                float_precision="high",
                chunksize=chunksize,
                low_memory=False,
            )
        ):
            work_data = pd.concat([work_data, chunk])
            if len(work_data) < filesize:
                continue
            do_work(work_dir, work_data, p)
            work_data = pd.DataFrame()
            p += 1

    if len(work_data) > 0:
        do_work(work_dir, work_data, p)
        work_data = pd.DataFrame()
    print(f"OK")

print("all done")