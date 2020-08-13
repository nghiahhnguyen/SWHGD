import csv
import gzip
from dateutil.parser import parse
from decimal import *
import pandas as pd
import gc
import os
from multiprocessing import Process, Pool
import random
total_snapshots = []
def get_project_id(name):
    snapshots = []
    info(name)
    for lines in pd.read_csv('/mnt/17volume/data/project-git-metrics-' + name +'.csv.gz', encoding='utf-8', header=None, chunksize=100000):
        for line in lines.iterrows():
            if line[1][0] not in snapshots:
                snapshots.append(int(line[1][0]))
    random_snapshots = random.sample(snapshots, 2000)
    df = pd.DataFrame(random_snapshots, columns=["snapshot_id"])
    df.to_csv('/home/sv/random_snapshot.csv', mode = 'a', index=False, header=False)

def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())
    
if __name__ == '__main__':
    p1 = Process(target=get_project_id, args = ('aa',))
    p1.start()
    p2 = Process(target=get_project_id, args = ('ab',))
    p2.start()
    p3 = Process(target=get_project_id, args = ('ac',))
    p3.start()
    p4 = Process(target=get_project_id, args = ('ad',))
    p4.start()
    p5 = Process(target=get_project_id, args = ('ae',))
    p5.start()
#     p6 = Process(target=get_project_id, args = ('af',))
#     p6.start()
