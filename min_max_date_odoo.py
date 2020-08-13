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
        
min_date = float('inf')
max_date = 0

def get_project_info(name, min_date, max_date):
    info(name)
    snapshots = {}
    snapshot_id = 0
    one_day = 86400
    one_week = 604800
    one_month = 2628000
    cnt = 0
    for lines in pd.read_csv('/mnt/17volume/data/snapshot.part0' + name, encoding='utf-8', header=None, chunksize=100000):
        for line in lines.iterrows():
            print(line[1][2])
            min_date = min(min_date, int(line[1][2]))
            max_date = max(max_date, int(line[1][2]))
            print('min_date', min_date)
            print('max_date', max_date)
def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())

    
if __name__ == '__main__': 
    p1 = Process(target=get_project_info, args=('0',min_date, max_date))
    p1.start()
    p2 = Process(target=get_project_info, args=('1',min_date, max_date))
    p2.start()
    p1.join()
    p2.join()
    with open('/home/sv/odoo_max_min_date.csv', mode = 'w') as snapshot_file:
        write = csv.writer(snapshot_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        write.writerow([min_date])
        write.writerow([max_date])
