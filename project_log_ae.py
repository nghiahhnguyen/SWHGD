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
    snapshots = {}
    snapshot_id = 0
    cnt = 0
    isOk = False
    for lines in pd.read_csv('/mnt/17volume/data/snapshot_revision_git.csv.gz.part' + name, encoding='utf-8', header=None, chunksize=100000):
        for line in lines.iterrows():
            if cnt == 0 and name == 'aa':
                print("first process: aa")
                cnt += 1
                continue
            if (line[1][0] == 60295038 and name == 'ae') or (line[1][0] == 60407141 and name == 'af'):
                    isOk = True
            if isOk == True:
                try:
                    if line[1][0] not in snapshots:
                        if len(snapshots) > 0:
                            one_day = 86400
                            one_week = 604800
                            one_month = 2628000
                            snapshots[snapshot_id].sort(key=lambda x: x.date)
                            min_date = snapshots[snapshot_id][0].date
                            max_date = snapshots[snapshot_id][len(snapshots[snapshot_id])-1].date
                            if max_date-min_date >= 7890000:
                                daily_wnd = []
                                weekly_wnd = []
                                monthly_wnd = []
                                base_date = min_date
                                while base_date <= max_date:
                                    base_date += one_day
                                    daily_wnd.append(base_date)
                                base_date = min_date
                                while base_date <= max_date:
                                    base_date += one_week
                                    weekly_wnd.append(base_date)
                                base_date = min_date
                                while base_date <= max_date:
                                    base_date += one_month
                                    monthly_wnd.append(base_date)

                                total_commits = 0
                                authors = []

                                daily_idx = 0
                                daily_commits = []
                                daily_contributors = []
                                daily_temp_contrib = []

                                weekly_idx = 0
                                weekly_commits = []
                                weekly_contributors = []
                                weekly_temp_contrib = []

                                monthly_idx = 0
                                monthly_commits = []
                                monthly_contributors = []
                                monthly_temp_contrib = []
                                for _, rev in enumerate(snapshots[snapshot_id]):

                                    total_commits += 1
                                    if rev.author not in authors:
                                        authors.append(rev.author)

                                    if rev.author not in daily_contributors and rev.date <= daily_wnd[0]:
                                        daily_contributors.append(rev.author)
                                    if rev.date <= daily_wnd[daily_idx]:
                                        if rev.author not in daily_temp_contrib:
                                            daily_temp_contrib.append(rev.author)
                                    else:
                                        daily_contributors = intersection(daily_contributors, daily_temp_contrib)
                                        daily_temp_contrib = []
                                        daily_idx += 1
                                        while daily_idx < len(daily_wnd) and rev.date > daily_wnd[daily_idx]:
                                            daily_commits.append(0)
                                            daily_contributors = []
                                            daily_idx += 1
                                        daily_temp_contrib.append(rev.author)
                                        daily_commits.append(1)

                                    if rev.author not in weekly_contributors and rev.date <= weekly_wnd[0]:
                                        weekly_contributors.append(rev.author)
                                    if rev.date <= weekly_wnd[weekly_idx]:
                                        if rev.author not in weekly_temp_contrib:
                                            weekly_temp_contrib.append(rev.author)
                                    else:
                                        weekly_contributors = intersection(weekly_contributors, weekly_temp_contrib)
                                        weekly_temp_contrib = []
                                        weekly_idx += 1
                                        while weekly_idx < len(weekly_wnd) and rev.date > weekly_wnd[weekly_idx]:
                                            weekly_commits.append(0)
                        #                     print("week")
                                            weekly_contributors = []
                                            weekly_idx += 1
                                        weekly_temp_contrib.append(rev.author)
                                        weekly_commits.append(1)

                                    if rev.author not in monthly_contributors and rev.date <= monthly_wnd[0]:
                                        monthly_contributors.append(rev.author)
                                    if rev.date <= monthly_wnd[monthly_idx]:
                                        if rev.author not in monthly_temp_contrib:
                                            monthly_temp_contrib.append(rev.author)
                                    else:
                                        monthly_contributors = intersection(monthly_contributors, monthly_temp_contrib)
                                        monthly_temp_contrib = []
                                        monthly_idx += 1
                                        while monthly_idx < len(monthly_wnd) and rev.date > monthly_wnd[monthly_idx]:
                                            monthly_commits.append(0)
                                            monthly_contributors = []
                                            monthly_idx += 1
                                        monthly_temp_contrib.append(rev.author)
                                        monthly_commits.append(1)
                                if len(daily_commits) == 0:
                                    daily_commits.append(1)
                                if len(weekly_commits) == 0:
                                    weekly_commits.append(1)
                                if len(monthly_commits) == 0:
                                    monthly_commits.append(1)
                               # if len(monthly_contributors) > 0:
                                daily_freq = Decimal(sum(daily_commits))/Decimal(len(daily_commits))
                                weekly_freq = Decimal(sum(weekly_commits))/Decimal(len(weekly_commits))
                                monthly_freq = Decimal(sum(monthly_commits))/Decimal(len(monthly_commits))
                                df = pd.DataFrame({
                                    'snapshot_id': [snapshot_id],
                                    'total_commits': [total_commits],
                                    'total_authors': [len(authors)],
                                    'daily_freq': [daily_freq],
                                    'daily_contributors': [len(daily_contributors)],
                                    'weekly_freq': [weekly_freq],
                                    'weekly_contributors': [len(weekly_contributors)],
                                    'monthly_freq': [monthly_freq],
                                    'monthly_contributors': [len(monthly_contributors)]
                                })
                                df.to_csv('/home/sv/project-git-metrics-' + name +'.csv.gz', compression = 'gzip', mode = 'a', header = False, index = False)
                                del daily_wnd
                                del weekly_wnd
                                del monthly_wnd
                                del daily_commits
                                del daily_contributors
                                del daily_temp_contrib
                                del weekly_commits
                                del weekly_contributors
                                del weekly_temp_contrib
                                del monthly_commits
                                del monthly_contributors
                                del monthly_temp_contrib
                            del snapshots
                            gc.collect()
                        snapshots = {}
                        snapshot_id = line[1][0]
                        if snapshot_id is not None: 
                            snapshots[snapshot_id] = [Revision(int(line[1][2]), int(line[1][3]), line[1][1].encode("utf-8"))]
                    else:
                        snapshots[line[1][0]].append(Revision(int(line[1][2]), int(line[1][3]), line[1][1].encode("utf-8")))
                except Exception as e:
                    f = open("/home/sv/project_error.txt", "a")
                    f.write("Project id: " + str(line[1][0]))
                    f.write(", error: " + str(e) + "\n")
                    with open('/home/sv/project_exception.csv', mode='a') as project_file:
                        project_writer = csv.writer(project_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                        project_writer.writerow([line[1][0], line[1][2], line[1][3], line[1][1]])
                    pass
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
#     p3 = Process(target=get_project_info, args = ('ac',))
#     p3.start()  
#     p4 = Process(target=get_project_info, args = ('ad',))
#     p4.start()  
    p5 = Process(target=get_project_info, args = ('ae',))
    p5.start()
    p6 = Process(target=get_project_info, args = ('af',))
    p6.start() 
