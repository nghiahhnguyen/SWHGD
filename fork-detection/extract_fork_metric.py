import pandas as pd
import numpy as np
from datetime import datetime
from decimal import Decimal


chunksize = 100000
snapshots = {}
snapshot_id = 0
one_month = 2629746
for lines in pd.read_csv('/home/sv/origin_fork_date.csv', encoding='utf-8', header=None, chunksize=100000):
        for line in lines.iterrows():
            if line[1][0] not in snapshots:
                if len(snapshots) > 0:
                    fork_cnt = 0
                    steady_month = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                    min_date = 1514764800
                    month_cnt = 0
                    snapshots[snapshot_id].sort()
                    for date in snapshots[snapshot_id]:
                        fork_cnt += 1
                        if date <= min_date + (month_cnt + 1)*one_month:
                            if month_cnt < 12:
                                steady_month[month_cnt] = 1
                            else:
                                print(date)
                        else:
                            month_cnt += 1
                            if month_cnt < 12:
                                steady_month[month_cnt] = 1
                            else:
                                print(date)
                    res = 1
                    if sum(steady_month) < 12:
                        res = 0
                    df = pd.DataFrame({
                        'origin_id': [snapshot_id],
                        'fork_counts': [fork_cnt],
                        'fork_frequency': [Decimal(sum(steady_month))/Decimal(len(steady_month))],
                        'steady_fork': [res]
                    })
                    df.to_csv('/home/sv/fork_metrics.csv', mode = 'a', header = None, index = False)
                snapshot_id = int(line[1][0])
                snapshots[snapshot_id] = [line[1][2]]
            else:
                snapshots[snapshot_id].append(line[1][2])

if len(snapshots) > 0:
    fork_cnt = 0
    steady_month = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    min_date = 1514764800
    month_cnt = 0
    snapshots[snapshot_id].sort()
    for date in snapshots[snapshot_id]:
        fork_cnt += 1
        if date <= min_date + (month_cnt + 1)*one_month:
            if month_cnt < 12:
                steady_month[month_cnt] = 1
            else:
                print(date)
        else:
            month_cnt += 1
            if month_cnt < 12:
                steady_month[month_cnt] = 1
            else:
                print(date)
    res = 1
    if sum(steady_month) < 12:
        res = 0
    df = pd.DataFrame({
        'origin_id': [snapshot_id],
        'fork_counts': [fork_cnt],
        'fork_frequency': [Decimal(sum(steady_month))/Decimal(len(steady_month))],
        'steady_fork': [res]
    })
    df.to_csv('/home/sv/fork_metrics.csv', mode = 'a', header = None, index = False)





