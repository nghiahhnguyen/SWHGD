import csv
from dateutil.parser import parse
from decimal import *
import pandas as pd
import gc
import os
from multiprocessing import Process
def intersection(list1, list2):
    res = []
    idx1 = 0
    while idx1 < len(list1):
        if list1[idx1] in list2:
            res.append(list1[idx1])
        idx1 += 1
    return res

class Revision:
    date = 1
    author = 1
    def __init__(self, date, author):
        self.date = date
        self.author = author

def get_project_info(name):
    info(name)
    snapshots = {}
    snapshot_id = 0
    one_day = 86400
    one_week = 604800
    one_month = 2628000
    cnt = 0
    for lines in pd.read_csv('/mnt/17volume/data/snapshot.part0' + name, encoding='utf-8', header=None, chunksize=100000):
        for line in lines.iterrows():
            try:
                if line[1][0] not in snapshots:
                    if line[1][0] == line[1][0]:
                        snapshot_id = line[1][0]
                        print('new snapshot:', line[1][0])
                        snapshots[line[1][0]] = [int(line[1][2]), int(line[1][3])]
                else:
                    snapshots[line[1][0]].append(int(line[1][2]), int(line[1][3]))
            except Exception as e:
                f = open("/home/sv/project_error.txt", "a")
                f.write("Project id: " + str(line[1][0]))
                f.write(", error: " + str(e) + "\n")
                with open('/home/sv/project_exception.csv', mode='a') as project_file:
                    project_writer = csv.writer(project_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    project_writer.writerow([line[1][0], line[1][2], line[1][3], line[1][1]])
                pass
    

    #do for the last one
    if len(snapshots) > 0:
        print("The last big snapshot:", snapshot_id)
        snapshots[snapshot_id].sort(key=lambda x: x[0])
        min_date = snapshots[snapshot_id][0][0]
        max_date = snapshots[snapshot_id][-1][0]
        if max_date-min_date >= 7890000:
            with open('/home/sv/big_snapshot_' + name + '.csv', mode = 'w') as snapshot_file:
                write = csv.writer(snapshot_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                write.writerows(snapshots[snapshot_id])
            print('done writing big snapshot:', snapshot_id)
            print('max_date:', max_date)
            print('min date:', min_date)
def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())

    
if __name__ == '__main__': 
    get_project_info('1')
