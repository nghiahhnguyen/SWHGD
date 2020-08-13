import pandas as pd
import numpy as np

snapshot_dicts = dict()                         
rows_to_keep = []
snapshot_fork_revision_duplicated_removed = pd.DataFrame(columns=["snapshot_id", "fork_id", "date"])
row_count = 0
chunksize = 100000
for chunk in pd.read_csv('/mnt/17volume/data/snapshot_fork.csv.gz', compression='gzip', names=["date", "fork_id", "snapshot_id",], header=None, chunksize=chunksize):
    revision_point = chunk
    revision_point = revision_point.reset_index()
    revision_point = revision_point.sort_values(by=['date'], ascending=False)
    for i, row in revision_point.iterrows():
        snapshot_id = row['snapshot_id']
        fork_snapshot_id = row['fork_id']
        if snapshot_id not in snapshot_dicts:
            snapshot_dicts[snapshot_id] = dict()
        snapshot_dict = snapshot_dicts[snapshot_id]
        if fork_snapshot_id not in snapshot_dict:
            snapshot_dict[fork_snapshot_id] = (i + row_count, row['date'])
        else:
            if snapshot_dict[fork_snapshot_id][1] < row['date']:
                snapshot_dict[fork_snapshot_id] = (i + row_count, row['date'])
    row_count += chunksize

rows_to_keep = []
for i, snapshot_dict in snapshot_dicts.items():
    for j, (fork_snapshot_row_idx, date) in snapshot_dict.items():
        rows_to_keep.append(fork_snapshot_row_idx)
print("Number of forks to keep", len(rows_to_keep))
iter_csv = pd.read_csv('/mnt/17volume/data/snapshot_fork.csv.gz', compression='gzip', header=None, chunksize=chunksize)
df = pd.concat([chunk[chunk.index.isin(rows_to_keep)] for chunk in iter_csv])
df.to_csv("/home/sv/data/snapshot_fork_duplicates_removed_fixed.csv.gz", compression="gzip", index=False)

#snapshot_fork_revision_duplicated_removed_chunk = revision_point.iloc[rows_to_keep].reset_index()
#snapshot_fork_revision_duplicated_removed = snapshot_fork_revision_duplicated_removed.append(snapshot_fork_revision_duplicated_removed_chunk)
#snapshot_fork_revision_duplicated_removed.to_csv("/home/sv/data/snapshot_fork_duplicates_removed.csv.gz", compression="gzip", index=False)

