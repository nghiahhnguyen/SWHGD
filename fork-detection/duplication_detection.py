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
    id = ""
    def __init__(self, date, id):
        self.date = date
        self.id = id

def get_project_info(name):
    info(name)
    snapshots = {}
    snapshot_id = 0
    for lines in pd.read_csv('/home/sv/origin_revision_data.csv', encoding='utf-8', usecols=range(6), header=None, chunksize=1000000):
        for line in lines.iterrows():
            try:
                if line[1][0] not in snapshots:
                    # if len(snapshots) > 0:
                    #     snapshots[snapshot_id].sort(key=lambda x: x[0])
                    snapshot_id = line[1][0]
                    if line[1][0] == line[1][0]:
                        print('new origin:', snapshot_id)
                        datetime = parse(line[1][3])
                        snapshots[line[1][0]] = [[datetime.timestamp(), line[1][5]]]
                else:
                    datetime = parse(line[1][3])
                    snapshots[line[1][0]].append([datetime.timestamp(), line[1][5]])
            except Exception as e:
                f = open("/home/sv/dup_error.txt", "a")
                f.write("Project id: " + str(line[1][0]))
                f.write(", error: " + str(e) + "\n")
                with open('/home/sv/dup_error.csv', mode='a') as project_file:
                    project_writer = csv.writer(project_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    project_writer.writerow([line[1]])
                pass
    # snapshots[snapshot_id].sort(key=lambda x: x[0])
    with open('/home/sv/dup_fork_date.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        for origin1,revisions1 in snapshots.items():
            originTable = {}
            for revision1 in revisions1:
                originTable[revision1[1]] = revision1[0]
            for origin2,revisions2 in snapshots.items():
                if origin1 != origin2:
                    for revision2 in reversed(revisions2):
                        if revision2[1] in originTable and revision2[0] >= originTable[revision2[1]]:
                            writer.writerow([origin1, origin2, revision2[0]])
                            break
            del originTable
            gc.collect()
def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())
if __name__ == '__main__': 
    get_project_info('')
