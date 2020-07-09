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
    cnt = 0
    snapshots = {}
    cycle_times = {}
    snapshot_id = 0
    for lines in pd.read_csv("/mnt/17volume/data/snapshot_revision_git.csv.gz.part" + name, chunksize=1000, encoding='utf-8', header=None):
        for line in lines.iterrows():
            #print(line[1])
            if cnt == 0 and name == 'aa':
                print("first process aa")
                cnt+=1
                continue
            try:
                if line[1][4] == line[1][4]:
                    if line[1][0] not in snapshots:
                        if len(snapshots) > 0:
                            snapshots[snapshot_id].sort()
                            #print(len(snapshots[snapshot_id]))                            
                            tmp = []
                            if len(snapshots[snapshot_id]) > 1:
                                for i in range(len(snapshots[snapshot_id])-1):
                                    tmp.append(abs(int(snapshots[snapshot_id][i]) - int(snapshots[snapshot_id][i+1])))
                                m = sum(tmp)/len(tmp)
                                cycle_times[snapshot_id] = m
                                #print(snapshot_id,' ', cycle_times[snapshot_id])
                                s  = pd.Series(cycle_times,index=cycle_times.keys())
                                df = pd.DataFrame({
                                    'snapshot_id':[snapshot_id], 
                                    'cycle_times':[m]
                                })
                                #print(df)
                                df.to_csv('/home/sv/cycle_time_release_' + name + '.csv.gz', compression = 'gzip', mode ='a', header=False, index=False)
                            del snapshots[snapshot_id]
                        del snapshots
                        gc.collect()
                        snapshots = {}
                        snapshot_id = line[1][0]
                        if snapshot_id is not None:
                            snapshots[snapshot_id] = [line[1][4]]
                    else:
                        snapshots[snapshot_id].append(line[1][4])
            except Exception as e:
                f = open("/home/sv/cycle_time_error.txt", "a")
                f.write("Project id: " + str(line[1][0]))
                f.write(", error: " + str(e) + "\n")
                with open('/home/sv/cycle_time_exception.csv', mode='a') as project_file:
                    project_writer = csv.writer(project_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    project_writer.writerow([line[1][0], line[1][1], line[1][2], line[1][3], line[1][4]])
                pass
def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())
    
if __name__ == '__main__':
    p1 = Process(target=calculate_cycle_times, args = ('aa',))
    p1.start()
    p2 = Process(target=calculate_cycle_times, args = ('ab',))
    p2.start()
    p3 = Process(target=calculate_cycle_times, args = ('ac',))
    p3.start()  
    p4 = Process(target=calculate_cycle_times, args = ('ad',))
    p4.start()  
    p5 = Process(target=calculate_cycle_times, args = ('ae',))
    p5.start()
    p6 = Process(target=calculate_cycle_times, args = ('af',))
    p6.start()

