import pandas as pd
import numpy as np
from datetime import datetime
from decimal import Decimal
import csv
import gc


chunksize = 100000
snapshots = {}
snapshot_id = 0
one_month = 2629746
with open("/mnt/17volume/databk/origin_dup_date_3.csv") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    try:
        for row in csv_reader:
            line_count += 1
            if len(row) == 3:
                if int(row[0]) not in snapshots:
                    if len(snapshots) > 0:
                        fork_cnt = 0
                        steady_month = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                        min_date = 1514764800
                        month_cnt = 0
                        snapshots[snapshot_id].sort()
                        for date in snapshots[snapshot_id]:
                            if date < 1548720000:
                                fork_cnt += 1
                            if date <= min_date + (month_cnt + 1)*one_month:
                                if month_cnt < 13:
                                    steady_month[month_cnt] = 1
                            else:
                                month_cnt += 1
                                if month_cnt < 13:
                                    steady_month[month_cnt] = 1
                        res = 1
                        if sum(steady_month) < 13:
                            res = 0
                        df = pd.DataFrame({
                            'origin_id': [snapshot_id],
                            'fork_counts': [fork_cnt],
                            'fork_frequency': [Decimal(sum(steady_month))/Decimal(len(steady_month))]
                        })
                        df.to_csv('/home/sv/dup_metrics_3.csv', mode = 'a', header = None, index = False)
                        del snapshots
                        gc.collect()
                        snapshots = {}
                    snapshot_id = int(row[0])
                    snapshots[snapshot_id] = [int(row[2])]
                else:
                    snapshots[snapshot_id].append(int(row[2]))
    except Exception:
        pass

if len(snapshots) > 0:
    fork_cnt = 0
    steady_month = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    min_date = 1514764800
    month_cnt = 0
    snapshots[snapshot_id].sort()
    for date in snapshots[snapshot_id]:
        if date < 1548720000:
            fork_cnt += 1
        if date <= min_date + (month_cnt + 1)*one_month:
            if month_cnt < 13:
                steady_month[month_cnt] = 1
        else:
            month_cnt += 1
            if month_cnt < 13:
                steady_month[month_cnt] = 1
    res = 1
    if sum(steady_month) < 13:
        res = 0
    df = pd.DataFrame({
        'origin_id': [snapshot_id],
        'fork_counts': [fork_cnt],
        'fork_frequency': [Decimal(sum(steady_month))/Decimal(len(steady_month))]
    })
    df.to_csv('/home/sv/dup_metrics_3.csv', mode = 'a', header = None, index = False)





