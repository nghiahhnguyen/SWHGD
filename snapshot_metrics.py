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
    url = ""
    isOk = False
    for lines in pd.read_csv('/home/sv/origin_revision_data.csv', encoding='utf-8', usecols=[0,1,2,3], header=None, chunksize=1000000):
        for line in lines.iterrows():
            if line[1][0] == 38833997:
                isOk = True
            if isOk is True:
                try:
                    if line[1][0] not in snapshots:
                        print(line[1][0])
                        if len(snapshots) > 0:
                            # snapshots[snapshot_id].sort(key=lambda x: x.date)
                            min_date = snapshots[snapshot_id][0].date
                            print('start extracting snapshot:', snapshot_id)
                            total_commits = 0
                            authors = []
                            
                            daily_cnt = 1
                            daily_commits = []
                            daily_contributors = []
                            daily_temp_contrib = []

                            weekly_cnt = 1
                            weekly_commits = []
                            weekly_contributors = []
                            weekly_temp_contrib = []

                            monthly_cnt = 1
                            monthly_commits = []
                            monthly_contributors = []
                            monthly_temp_contrib = []
                            
                            for _, rev in enumerate(snapshots[snapshot_id]):
                                total_commits += 1
                                if rev.author not in authors:
                                    authors.append(rev.author)
                                    
                                #daily
                                if rev.author not in daily_contributors and rev.date <= min_date + one_day:
                                    daily_contributors.append(rev.author)
                                if rev.date <= min_date + one_day*daily_cnt:
                                    if rev.author not in daily_temp_contrib:
                                        daily_temp_contrib.append(rev.author)
                                else:
                                    daily_contributors = intersection(daily_contributors, daily_temp_contrib)
                                    daily_temp_contrib = []
                                    daily_cnt += 1
                                    while rev.date > min_date + one_day*daily_cnt:
                                        daily_commits.append(0)
                                        daily_contributors = []
                                        daily_cnt += 1
                                    daily_temp_contrib.append(rev.author)
                                    daily_commits.append(1)
                                
                                #weekly
                                if rev.author not in weekly_contributors and rev.date <= min_date + one_week:
                                    weekly_contributors.append(rev.author)
                                if rev.date <= min_date + one_week*weekly_cnt:
                                    if rev.author not in weekly_temp_contrib:
                                        weekly_temp_contrib.append(rev.author)
                                else:
                                    weekly_contributors = intersection(weekly_contributors, weekly_temp_contrib)
                                    weekly_temp_contrib = []
                                    weekly_cnt += 1
                                    while rev.date > min_date + one_week*weekly_cnt:
                                        weekly_commits.append(0)
                                        weekly_contributors = []
                                        weekly_cnt += 1
                                    weekly_temp_contrib.append(rev.author)
                                    weekly_commits.append(1)
                                
                                #monthly
                                if rev.author not in monthly_contributors and rev.date <= min_date + one_month:
                                    monthly_contributors.append(rev.author)
                                if rev.date <= min_date + one_month*monthly_cnt:
                                    if rev.author not in monthly_temp_contrib:
                                        monthly_temp_contrib.append(rev.author)
                                else:
                                    monthly_contributors = intersection(monthly_contributors, monthly_temp_contrib)
                                    monthly_temp_contrib = []
                                    monthly_cnt += 1
                                    while rev.date > min_date + one_month*monthly_cnt:
                                        monthly_commits.append(0)
                                        monthly_contributors = []
                                        monthly_cnt += 1
                                    monthly_temp_contrib.append(rev.author)
                                    monthly_commits.append(1)

                            if len(daily_commits) == 0:
                                daily_commits.append(1)
                            if len(weekly_commits) == 0:
                                weekly_commits.append(1)
                            if len(monthly_commits) == 0:
                                monthly_commits.append(1)
                            daily_freq = Decimal(sum(daily_commits))/Decimal(len(daily_commits))
                            weekly_freq = Decimal(sum(weekly_commits))/Decimal(len(weekly_commits))
                            monthly_freq = Decimal(sum(monthly_commits))/Decimal(len(monthly_commits))
                            df = pd.DataFrame({
                                'origin_id': [snapshot_id],
                                'url': [url],
                                'total_commits': [total_commits],
                                'total_authors': [len(authors)],
                                'daily_freq': [daily_freq],
                                'daily_contributors': [len(daily_contributors)],
                                'weekly_freq': [weekly_freq],
                                'weekly_contributors': [len(weekly_contributors)],
                                'monthly_freq': [monthly_freq],
                                'monthly_contributors': [len(monthly_contributors)]
                            })
                            df.to_csv('/home/sv/origin-metrics.csv', mode = 'a', header = False, index = False)
                            print('done writing snapshot:', snapshot_id)
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
                        url = line[1][1]
                        if line[1][0] == line[1][0]:
                            print('new origin:', snapshot_id)
                            datetime = parse(line[1][3])
                            snapshots[line[1][0]] = [Revision(datetime.timestamp(), int(line[1][2]))]
                    else:
                        datetime = parse(line[1][3])
                        snapshots[line[1][0]].append(Revision(datetime.timestamp(), int(line[1][2])))
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
        print("The fucking last snapshot:", snapshot_id)
        snapshots[snapshot_id].sort(key=lambda x: x.date)
        min_date = snapshots[snapshot_id][0].date
        print('start extracting snapshot:', snapshot_id)
        total_commits = 0
        authors = []

        daily_cnt = 1
        daily_commits = []
        daily_contributors = []
        daily_temp_contrib = []

        weekly_cnt = 1
        weekly_commits = []
        weekly_contributors = []
        weekly_temp_contrib = []

        monthly_cnt = 1
        monthly_commits = []
        monthly_contributors = []
        monthly_temp_contrib = []

        for _, rev in enumerate(snapshots[snapshot_id]):
            total_commits += 1
            if rev.author not in authors:
                authors.append(rev.author)

            #daily
            if rev.author not in daily_contributors and rev.date <= min_date + one_day:
                daily_contributors.append(rev.author)
            if rev.date <= min_date + one_day*daily_cnt:
                if rev.author not in daily_temp_contrib:
                    daily_temp_contrib.append(rev.author)
            else:
                daily_contributors = intersection(daily_contributors, daily_temp_contrib)
                daily_temp_contrib = []
                daily_cnt += 1
                while rev.date > min_date + one_day*daily_cnt:
                    daily_commits.append(0)
                    daily_contributors = []
                    daily_cnt += 1
                daily_temp_contrib.append(rev.author)
                daily_commits.append(1)

            #weekly
            if rev.author not in weekly_contributors and rev.date <= min_date + one_week:
                weekly_contributors.append(rev.author)
            if rev.date <= min_date + one_week*weekly_cnt:
                if rev.author not in weekly_temp_contrib:
                    weekly_temp_contrib.append(rev.author)
            else:
                weekly_contributors = intersection(weekly_contributors, weekly_temp_contrib)
                weekly_temp_contrib = []
                weekly_cnt += 1
                while rev.date > min_date + one_week*weekly_cnt:
                    weekly_commits.append(0)
                    weekly_contributors = []
                    weekly_cnt += 1
                weekly_temp_contrib.append(rev.author)
                weekly_commits.append(1)

            #monthly
            if rev.author not in monthly_contributors and rev.date <= min_date + one_month:
                monthly_contributors.append(rev.author)
            if rev.date <= min_date + one_month*monthly_cnt:
                if rev.author not in monthly_temp_contrib:
                    monthly_temp_contrib.append(rev.author)
            else:
                monthly_contributors = intersection(monthly_contributors, monthly_temp_contrib)
                monthly_temp_contrib = []
                monthly_cnt += 1
                while rev.date > min_date + one_month*monthly_cnt:
                    monthly_commits.append(0)
                    monthly_contributors = []
                    monthly_cnt += 1
                monthly_temp_contrib.append(rev.author)
                monthly_commits.append(1)

        if len(daily_commits) == 0:
            daily_commits.append(1)
        if len(weekly_commits) == 0:
            weekly_commits.append(1)
        if len(monthly_commits) == 0:
            monthly_commits.append(1)
        daily_freq = Decimal(sum(daily_commits))/Decimal(len(daily_commits))
        weekly_freq = Decimal(sum(weekly_commits))/Decimal(len(weekly_commits))
        monthly_freq = Decimal(sum(monthly_commits))/Decimal(len(monthly_commits))
        df = pd.DataFrame({
            'origin_id': [snapshot_id],
            'url': [url],
            'total_commits': [total_commits],
            'total_authors': [len(authors)],
            'daily_freq': [daily_freq],
            'daily_contributors': [len(daily_contributors)],
            'weekly_freq': [weekly_freq],
            'weekly_contributors': [len(weekly_contributors)],
            'monthly_freq': [monthly_freq],
            'monthly_contributors': [len(monthly_contributors)]
        })
        df.to_csv('/home/sv/origin-metrics.csv', mode = 'a', header = False, index = False)
        print('done writing snapshot:', snapshot_id)
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
def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())
if __name__ == '__main__': 
    get_project_info('')
