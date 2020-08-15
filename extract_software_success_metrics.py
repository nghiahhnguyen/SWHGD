import csv
import pandas as pd
import gc
import os
from multiprocessing import Process

class Metric:
    url = ""
    stargazer_counts = 0
    watcher_counts = 0
    fork_counts = 0
    subscriber_counts = 0
    commit_after_fork_counts = 0
    contributor_after_fork_counts = 0
id_to_urls = {}
url_to_ids = {}
snapshots = {}

def get_snapshot_urls(filename):
    print("get snapshot urls")
    for lines in pd.read_csv(filename, encoding='utf-8', header=None, chunksize=100000):
        for line in lines.iterrows():
            url_to_ids[line[1][1]] = int(line[1][0])
            snapshots[int(line[1][0])] = Metric()
            

def get_success_metrics(filename):
    print("get success metrics")
    cnt = 0
    with open(filename, 'r') as f:
        data = csv.reader(f)
        for row in data:
            if row[0] in url_to_ids and len(row) >= 5:
                snapshot_id = url_to_ids[row[0]]
                snapshots[snapshot_id].url = row[0]
                if row[1] != '':
                    print(row[1])
                    snapshots[snapshot_id].stargazer_counts = int(row[1])
                else:
                    snapshots[snapshot_id].stargazer_counts = 0

                if row[2] != '':
                    snapshots[snapshot_id].watcher_counts = int(row[2])
                else:
                    snapshots[snapshot_id].watcher_counts = 0

                if row[3] != '':
                    snapshots[snapshot_id].fork_counts = int(row[3])
                else:
                    snapshots[snapshot_id].fork_counts = 0

                if row[4] != '':
                    snapshots[snapshot_id].subscriber_counts = int(row[4])
                else:
                    snapshots[snapshot_id].subscriber_counts = 0
  
    # for lines in pd.read_csv(filename, encoding='utf-8', header=None, chunksize=1):
    #     for line in lines.iterrows():
            # if cnt == 0:
            #     cnt += 1
            #     continue
            # cnt += 1
            # if cnt == 30092:
            # print(line)
            # snapshot_id = url_to_ids[line[1][0]]
            # snapshots[snapshot_id].url = line[1][0]
            # if line[1][1] == line[1][1]:
            #     snapshots[snapshot_id].stargazer_counts = int(line[1][1])
            # else:
            #     snapshots[snapshot_id].stargazer_counts = 0

            # if line[1][2] == line[1][2]:
            #     snapshots[snapshot_id].watcher_counts = int(line[1][2])
            # else:
            #     snapshots[snapshot_id].watcher_counts = 0

            # if line[1][3] == line[1][3]:
            #     snapshots[snapshot_id].fork_counts = int(line[1][3])
            # else:
            #     snapshots[snapshot_id].fork_counts = 0

            # if line[1][4] == line[1][4]:
            #     snapshots[snapshot_id].subscriber_counts = int(line[1][4])
            # else:
            #     snapshots[snapshot_id].subscriber_counts = 0
check_snapshots = set()
def get_community_fork_metrics(filename):
    print("get community fork metrics")
    for lines in pd.read_csv(filename, encoding='utf-8', header=None, chunksize=100000):
        for line in lines.iterrows():
            snapshot_id = int(line[1][0])
            check_snapshots.add(snapshot_id)
            if snapshot_id in snapshots:
                snapshots[snapshot_id].contributor_after_fork_counts = int(line[1][1])
                snapshots[snapshot_id].commit_after_fork_counts = int(line[1][2])
        

#read snapshot urls ids
get_snapshot_urls('/home/sv/origin_url_data.csv')
get_community_fork_metrics('/home/sv/fork_parameters_3.csv')
get_success_metrics('/home/sv/origin_urls_data.csv')

metrics = []
for snapshot_id in snapshots:
    if snapshot_id in check_snapshots:
        metric = snapshots[snapshot_id]
        if metric.stargazer_counts > 0 and metric.fork_counts > 0:
            metrics.append([snapshot_id, metric.url, metric.stargazer_counts,\
            metric.watcher_counts, metric.fork_counts,\
            metric.subscriber_counts,\
                metric.commit_after_fork_counts, metric.contributor_after_fork_counts])

df = pd.DataFrame(metrics,columns=[
    'origin',
    'url',
    'stargazer_counts',
    'watcher_counts',
    'fork_counts',
    'subscriber_counts',
    'commit_after_fork_counts',
    'contributor_after_fork_counts'
])

df.to_csv('/home/sv/community_fork_metrics_3.csv',index=False,header=True)
