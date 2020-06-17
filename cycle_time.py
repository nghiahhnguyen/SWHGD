import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb


cnt = 0

snapshots = {}
cycle_times = {}
snapshot_id = 0
#for lines in pd.read_csv("/mnt/17volume/data/snapshot_revision_git.csv.gz", compression='gzip', chunksize=1000, encoding='utf-8', header=None):
for lines in pd.read_csv("/mnt/17volume/data/snapshot_revision_git.csv.gz", compression='gzip', chunksize=1000, encoding='utf-8', header=None):
    for line in lines.iterrows():
        #print(line[1])
        if cnt == 0:
            cnt+=1
            continue
        if line[1][4] == line[1][4]:
            if line[1][0] not in snapshots:
                if len(snapshots) > 0:
                    snapshots[snapshot_id].sort()
                    #print(len(snapshots[snapshot_id]))
                    tmp = []
                    if len(snapshots[snapshot_id]) > 1:
                        for i in range(len(snapshots[snapshot_id])-1):
                            tmp.append(abs(snapshots[snapshot_id][i] - snapshots[snapshot_id][i+1]))
                        m = sum(tmp)/len(tmp)
                        cycle_times[snapshot_id] = m
                        #print(snapshot_id,' ', cycle_times[snapshot_id])
                        s  = pd.Series(cycle_times,index=cycle_times.keys())
                        df = pd.DataFrame(s.items(), columns=['snapshot_id', 'cycle_time'])
                        #print(df)
                        df.to_csv('/home/sv/cycle_time.csv.gz', compression = 'gzip', mode ='w', header=True, index=False)
                    del snapshots[snapshot_id]
                    
                snapshot_id = line[1][0]
                snapshots[snapshot_id] = [line[1][4]]
            else:
                snapshots[snapshot_id].append(line[1][4])
