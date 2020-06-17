import numpy as np
import pandas as pd
from datetime import datetime

df = pd.read_csv("/home/sv/data/snapshot_fork_duplicates_removed.csv.gz", header=None, compression="gzip", names=["date", "fork_id", "snapshot_id"])
df['year'] = df.apply(lambda row: datetime.utcfromtimestamp(row['date']).year, axis=1)
df['month'] = df.apply(lambda row: datetime.utcfromtimestamp(row['date']).month, axis=1)
df['day'] = df.apply(lambda row: datetime.utcfromtimestamp(row['date']).day, axis=1)

snapshot_fork_dict = dict()
for i, row in df.iterrows():
    if row.snapshot_id not in snapshot_fork_dict:
        snapshot_fork_dict[row.snapshot_id] = dict()
    if row.year not in snapshot_fork_dict[row.snapshot_id]:
        snapshot_fork_dict[row.snapshot_id][row.year] = 1
    else:
        snapshot_fork_dict[row.snapshot_id][row.year] += 1

for snapshot, revision_date_set in snapshot_fork_dict.items():
    print("Snapshot:", snapshot, "Number of forks:", len(revision_date_set))

for snapshot, revision_date_set in snapshot_fork_dict.items():
    print(snapshot)
    for year, frequency in revision_date_set.items():
        print("Year:", year, "Frequency:", frequency)


