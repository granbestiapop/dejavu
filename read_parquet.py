import pyarrow.parquet as pq

table = pq.read_table('parquet_dataset/16.parquet')
print(table.to_pandas())