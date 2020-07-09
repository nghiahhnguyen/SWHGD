import csv
import gzip
from dateutil.parser import parse
from decimal import *
import pandas as pd
import gc
import threading
import os
from multiprocessing import Process
class my_thread(threading.Thread):
    def __init__(self, thread_id, name):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = name
    def run(self):
        print("Starting " + self.name)
        get_project_info(self.name)
        print("Exiting " + self.name)
def intersection(list1, list2):
    res = []
    idx1 = 0
    while idx1 < len(list1):
        if list1[idx1] in list2:
            res.append(list1[idx1])
        idx1 += 1
    return res

class Revision:
    id = ""
    date = 1
    author = 1
    def __init__(self, date, author, id):
        self.date = date
        self.author = author
        self.id = id
def get_project_info(name):
    info(name)
    snapshots = set()
    snapshot_id = 0
    cnt = 0
    isOk = False
    for lines in pd.read_csv('/mnt/17volume/data/snapshot_revision_git.csv.gz.part' + name, encoding='utf-8', header=None, chunksize=100000):
        for line in lines.iterrows():
            if line[1][0] == 57426990:
                    isOk = True
            if isOk == True:
                if line[1][0] not in snapshots:
                    snapshots.add(int(line[1][0]))
                    
    for snapshot in snapshots:
        df = pd.DataFrame({'snapshot_id': [snapshot]})
    df.to_csv('/home/sv/snapshot_rest.csv', mode ='w', header=False, index=False)
                        
def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())
# try:
#     thread1 = my_thread(1, "aa")
#     thread2 = my_thread(2, "ab")
#     thread3 = my_thread(3, "ac")
#     thread4 = my_thread(4, "ad")
#     thread5 = my_thread(5, "ae")
#     thread6 = my_thread(6, "af")
#     thread1.start()
#     thread2.start()
#     thread3.start()
#     thread4.start()
#     thread5.start()
#     thread6.start()
# except:
#     print("Error: unable to start thread")
if __name__ == '__main__':
#     p1 = Process(target=get_project_info, args = ('aa',))
#     p1.start()
#     p2 = Process(target=get_project_info, args = ('ab',))
#     p2.start()
    p3 = Process(target=get_project_info, args = ('ac',))
    p3.start()
#     p4 = Process(target=get_project_info, args = ('ad',))
#     p4.start()  
#     p5 = Process(target=get_project_info, args = ('ae',))
#     p5.start()
#     p6 = Process(target=get_project_info, args = ('af',))
#     p6.start() 
