import csv
from dateutil.parser import parse
from decimal import *
import pandas as pd
import gc
import os
from multiprocessing import Process

class Metric:
    fork_counts = 0
    steady_fork = 0
    fork_frequency = 0.0
    commit_counts = 0
    contributor_counts = 0
    daily_commit_frequency = 0.0
    weekly_commit_frequency = 0.0
    monthly_commit_frequency = 0.0
    daily_contributor_counts = 0
    weekly_contributor_counts = 0
    monthly_contributor_counts = 0
    cycle_time = 0.0
snapshot_ids = []
snapshots = {}
def get_fork_metrics():
    for lines in pd.read_csv('/home/sv/dup_metrics.csv', encoding='utf-8', header=None, chunksize=100000):
        for line in lines.iterrows():
            snapshot_id = int(line[1][0])
            snapshots[snapshot_id] = Metric()
            snapshots[snapshot_id].fork_counts = int(line[1][1])
            snapshots[snapshot_id].fork_frequency = line[1][2]
            snapshots[snapshot_id].steady_fork = int(line[1][3])

def get_software_health_risk_metrics(filename):
    print(filename)
    cnt = 0
    for lines in pd.read_csv('/home/sv/origin-metrics.csv', encoding='utf-8', header=None, chunksize=100000):
        for line in lines.iterrows():
            if int(line[1][0]) in snapshots:
                cnt += 1
                snapshot_id = int(line[1][0])
                print('Number:', cnt, 'snapshot:', snapshot_id)
                snapshots[snapshot_id].commit_counts = line[1][2]
                snapshots[snapshot_id].contributor_counts = line[1][3]
                snapshots[snapshot_id].daily_commit_frequency = line[1][4]
                snapshots[snapshot_id].weekly_commit_frequency = line[1][6]
                snapshots[snapshot_id].monthly_commit_frequency = line[1][8]
                snapshots[snapshot_id].daily_contributor_counts = line[1][5]
                snapshots[snapshot_id].weekly_contributor_counts = line[1][7]
                snapshots[snapshot_id].monthly_contributor_counts = line[1][9]
    return cnt

def get_longevity_metric(filename):
    print("get longevity")
    for lines in pd.read_csv('/mnt/17volume/data/' + filename, encoding='utf-8', header=None, chunksize=100000):
        for line in lines.iterrows():
            if int(line[1][0]) in snapshots:
                snapshot_id = int(line[1][0])
                snapshots[snapshot_id].longevity = line[1][1]

def get_cycle_time_metric(filename):
    print("get cycle time")
    for lines in pd.read_csv('/home/sv/' + filename, encoding='utf-8', header=None, chunksize=100000):
        for line in lines.iterrows():
            if int(line[1][0]) in snapshots:
                snapshot_id = int(line[1][0])
                snapshots[snapshot_id].cycle_time = line[1][2]


#read fork metrics
get_fork_metrics()
# read software health and risk metrics
get_software_health_risk_metrics('project-git-metrics-first.csv')
# get_software_health_risk_metrics('project-git-metrics-aa.csv')
# get_software_health_risk_metrics('project-git-metrics-ab.csv')
# get_software_health_risk_metrics('project-git-metrics-ac.csv')
# get_software_health_risk_metrics('project-git-metrics-ad.csv')
# get_software_health_risk_metrics('project-git-metrics-ae.csv')
# get_software_health_risk_metrics('project-git-metrics-af.csv')

#read cycle time metric
get_cycle_time_metric('cycle_time_release.csv')
# get_cycle_time_metric('cycle_time_release_aa.csv')
# get_cycle_time_metric('cycle_time_release_ab.csv')
# get_cycle_time_metric('cycle_time_release_ac.csv')
# get_cycle_time_metric('cycle_time_release_ad.csv')
# get_cycle_time_metric('cycle_time_release_ae.csv')
# get_cycle_time_metric('cycle_time_release_af.csv')
# write to file
metrics = []
for snapshot_id in snapshots:
    metric = snapshots[snapshot_id]
    metrics.append([metric.fork_counts, metric.fork_frequency,\
    metric.steady_fork,\
    metric.commit_counts,\
    metric.contributor_counts, metric.daily_commit_frequency,\
    metric.weekly_commit_frequency, metric.monthly_commit_frequency,\
    metric.daily_contributor_counts, metric.weekly_contributor_counts,\
    metric.monthly_contributor_counts,\
        metric.cycle_time])

print(len(metrics))

df = pd.DataFrame(metrics,columns=['fork_counts',\
'fork_frequency',\
'steady_fork',\
'commit_counts',\
'contributor_counts',\
'daily_commit_frequency',\
'weekly_commit_frequency',\
'monthly_commit_frequency',\
'daily_contributor_counts',\
'weekly_contributor_counts',\
'monthly_contributor_counts',\
'cycle_time'
    ])

df.to_csv('/home/sv/dup_health_risk.csv',index=False,header=True)


