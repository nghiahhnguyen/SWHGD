import csv
import gzip
from dateutil.parser import parse
from decimal import *
import pandas as pd
import gc
import os
from multiprocessing import Process
def calculate_cycle_times(name):
    info(name)
    origins = {}
    origin_id = 0
    url = ""
    check_snapshots = set()
    with open("/home/sv/fork_metrics_3.csv") as csv_file: 
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            check_snapshots.add(int(row[0]))
    with open("/home/sv/origin_release_data_3.csv") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            try:
                if int(row[0]) not in origins:
                    if len(origins) > 0:
                        print("Origin 1: ", origin_id)
                        origins[origin_id].sort()                            
                        tmp = []
                        res = 0.0
                        if len(origins[origin_id]) > 1:
                            for i in range(len(origins[origin_id])-1):
                                tmp.append(abs(int(origins[origin_id][i]) - int(origins[origin_id][i+1])))
                            m = sum(tmp)/len(tmp)
                            m = m / (24.0*3600.0)
                        else:
                            m = 1095
                        df = pd.DataFrame({
                            'origin_id':[origin_id], 
                            'snapshot_id': [url],
                            'cycle_time':[m]
                        })
                        df.to_csv('/home/sv/cycle_time_release_3.csv', mode ='a', header=False, index=False)
                        del origins
                        gc.collect()
                    origins = {}
                    url = int(row[1])
                    origin_id = int(row[0])
                    if origin_id == origin_id:
                        datetime = parse(row[4])
                        if datetime.timestamp() < 1517227200:
                            origins[origin_id] = [datetime.timestamp()]
                else:
                    datetime = parse(row[4])
                    if datetime.timestamp() < 1517227200:
                        origins[origin_id].append(datetime.timestamp())
            except Exception as e:
                print(e)
                pass
    if len(origins) > 0:
        print("Origin: ", origin_id)
        origins[origin_id].sort()                            
        tmp = []
        res = 0.0
        if len(origins[origin_id]) > 1:
            for i in range(len(origins[origin_id])-1):
                tmp.append(abs(int(origins[origin_id][i]) - int(origins[origin_id][i+1])))
            m = sum(tmp)/len(tmp)
            m = m / (24.0*3600.0)
        else:
            m = 1095
        df = pd.DataFrame({
            'origin_id':[origin_id], 
            'snapshot_id': [url],
            'cycle_time':[m]
        })
        df.to_csv('/home/sv/cycle_time_release_3.csv', mode ='a', header=False, index=False)
    del origins
    gc.collect()
    
def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())
    
if __name__ == '__main__':
    calculate_cycle_times('')

    

