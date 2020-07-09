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
    cnt = 0
    snapshots = {}
    longevity = {}
    snapshot_id = 0
    for lines in pd.read_csv("/mnt/17volume/data/snapshot_revision_git.csv.gz.part" + name, chunksize=1000, encoding='utf-8', header=None):
        for line in lines.iterrows():
            #print(line[1])
            if cnt == 0 and name == 'aa':
                print("first process aa")
                cnt+=1
                continue
            try:    
                if line[1][2] == line[1][2]:
                    if line[1][0] not in snapshots:
                        if len(snapshots) > 0:
                            snapshots[snapshot_id].sort()
                            #print(len(snapshots[snapshot_id]))
                            if len(snapshots[snapshot_id]) > 1:
                                m = abs(int(snapshots[snapshot_id][-1]) - int(snapshots[snapshot_id][0]) )

                                #print(snapshot_id,' ', longevity[snapshot_id])
                                s  = pd.Series(longevity,index=longevity.keys())
                                df = pd.DataFrame({
                                    'snapshot_id':[snapshot_id], 
                                    'longevity':[m]
                                })
                                #print(df)
                                df.to_csv('/home/sv/longevity-' + name + '.csv.gz', compression = 'gzip', mode ='a', header=False, index=False)
                            del snapshots[snapshot_id]
                        del snapshots
                        gc.collect()
                        snapshots = {}
                        snapshot_id = line[1][0]
                        if snapshot_id is not None:
                            snapshots[snapshot_id] = [line[1][2]]
                    else:
                        snapshots[snapshot_id].append(line[1][2])
            except Exception as e:
                f = open("/home/sv/longevity_error.txt", "a")
                f.write("Project id: " + str(line[1][0]))
                f.write(", error: " + str(e) + "\n")
                with open('/home/sv/longevity_exception.csv', mode='a') as project_file:
                    project_writer = csv.writer(project_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    project_writer.writerow([line[1][0], line[1][2], line[1][3], line[1][1]])
                pass
def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())
    
if __name__ == '__main__':
    p1 = Process(target=calculate_longevity, args = ('aa',))
    p1.start()
    p2 = Process(target=calculate_longevity, args = ('ab',))
    p2.start()
    p3 = Process(target=calculate_longevity, args = ('ac',))
    p3.start()  
    p4 = Process(target=calculate_longevity, args = ('ad',))
    p4.start()  
    p5 = Process(target=calculate_longevity, args = ('ae',))
    p5.start()
    p6 = Process(target=calculate_longevity, args = ('af',))
    p6.start()
