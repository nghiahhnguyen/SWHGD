import csv
import gzip
from dateutil.parser import parse
from decimal import *
import pandas as pd
import gc
import os
from multiprocessing import Process
def calculate_longevity(name):
    info(name)
    snapshots = {}
    longevity = {}
    snapshot_id = 0
    isOk = False
    for lines in pd.read_csv("/mnt/17volume/data/longevity.part01", chunksize=100000, encoding='utf-8', header=None):
        for line in lines.iterrows():
            try:
                if line[1][0] == line[1][0] and line[1][0] == 60296782:
                    isOk = True
                if isOk == True:
                    if line[1][2] == line[1][2]:
                        if line[1][0] not in snapshots:
                            if len(snapshots) > 0:
                                print("Snapshot:", snapshot_id)
                                if len(snapshots[snapshot_id]) > 1:
                                    m = abs(int(snapshots[snapshot_id][1]) - int(snapshots[snapshot_id][0]))
                                    s  = pd.Series(longevity,index=longevity.keys())
                                    df = pd.DataFrame({
                                        'snapshot_id':[snapshot_id], 
                                        'longevity':[m]
                                    })
                                    df.to_csv('/home/sv/longevity-first.csv', mode ='a', header=False, index=False)
                                del snapshots[snapshot_id]
                            del snapshots
                            gc.collect()
                            snapshots = {}
                            snapshot_id = line[1][0]
                            if snapshot_id == snapshot_id:
                                snapshots[snapshot_id] = [line[1][2], line[1][2]]
                        else:
                            snapshots[snapshot_id][0] = min(line[1][2], snapshots[snapshot_id][0])
                            snapshots[snapshot_id][1] = max(line[1][2], snapshots[snapshot_id][1])
            except Exception as e:
                f = open("/home/sv/longevity_error.txt", "a")
                f.write("Project id: " + str(line[1][0]))
                f.write(", error: " + str(e) + "\n")
                with open('/home/sv/longevity_exception.csv', mode='a') as project_file:
                    project_writer = csv.writer(project_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    project_writer.writerow([line[1][0], line[1][2], line[1][3], line[1][1]])
                pass
    #the last one
    if len(snapshots) > 0:
        print("The last snapshot:", snapshot_id)
        if len(snapshots[snapshot_id]) > 1:
            m = abs(int(snapshots[snapshot_id][1]) - int(snapshots[snapshot_id][0]))
            s  = pd.Series(longevity,index=longevity.keys())
            df = pd.DataFrame({
                'snapshot_id':[snapshot_id], 
                'longevity':[m]
            })
            df.to_csv('/home/sv/longevity-first.csv', mode ='a', header=False, index=False)
def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())
    
if __name__ == '__main__':
    calculate_longevity('')
    
