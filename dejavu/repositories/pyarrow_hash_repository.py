from typing import List, Tuple, Dict, Set
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from dejavu.repositories.hash_repository import HashRepository

class ParquetHashRepository(HashRepository):
    def __init__(self):
        super().__init__()
    
    def insert_hashes(self, song_id: int, hashes: List[Tuple[str, int]], batch_size: int = 1000) -> None:
        hash = [hsh.upper() for hsh, _ in hashes]
        offset = [offset for _, offset in hashes]
        song = [song_id] * len(hashes)
        df = pd.DataFrame({'hash': hash, 'offset': offset, 'song_id': song})
        table = pa.Table.from_pandas(df)
        pq.write_table(table, "parquet_dataset/" + str(song_id) + '.parquet')

    def return_matches(self, hashes: List[Tuple[str, int]],
                       batch_size: int = 1000) -> Tuple[List[Tuple[int, int]], Dict[int, int]]:
        # Move this shit to other place
        mapper = {}
        for hsh, offset in hashes:
            if hsh.upper() in mapper.keys():
                mapper[hsh.upper()].append(offset)
            else:
                mapper[hsh.upper()] = [offset]

        values = list(mapper.keys())
                # in order to count each hash only once per db offset we use the dic below
        dedup_hashes = {}

        results = []
        res = self.get_hashes(values)
        print("parquet len:" + str(len(list(res))))

        for hsh, sid, offset in res:
            if sid not in dedup_hashes.keys():
                dedup_hashes[sid] = 1
            else:
                dedup_hashes[sid] += 1
            #  we now evaluate all offset for each  hash matched
            for song_sampled_offset in mapper[hsh.upper()]:
                results.append((sid, offset - song_sampled_offset))
        return results, dedup_hashes


    def get_hashes(self, hashes: Set[str]): 
        dataset = ds.dataset("parquet_dataset", format="parquet")
        f = pc.is_in(ds.field('hash'), pa.array(hashes, pa.string()))
        results = dataset.to_table(filter = f).to_pydict()
        tuples = zip(results["hash"], results["song_id"], results["offset"])
        return list(tuples)
