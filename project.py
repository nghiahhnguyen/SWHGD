import csv
import gzip
import pandas as pd

def get_project_info():
    cnt = 0
    for lines in pd.read_csv('/mnt/17volume/data/snapshot_revision_git.csv.gz', compression= 'gzip', encoding='utf-8', header=None, low_memory=False, chunksize=1000000):
        for line in lines.iterrows():
            if cnt == 0:
                cnt += 1
                continue
            snapshot_id = int(line[1][0])
            revision_id = line[1][1]
            df = pd.DataFrame({
                'snapshot_id': [snapshot_id],
                'revision_id': [revision_id]
            })
            df.to_csv('/mnt/17volume/data/snapshot_revision.csv.gz', compression = 'gzip', mode = 'a', header = False, index = False)

get_project_info()