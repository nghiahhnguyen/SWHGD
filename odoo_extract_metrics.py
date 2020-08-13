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

def get_project_info():
    snapshot_id = 60295045
    one_day = 86400
    one_week = 604800
    one_month = 2628000
    cnt = 0
    min_date = 1165524100
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
    for lines in pd.read_csv('/home/sv/big_snapshot_.csv', encoding='utf-8', header=None, chunksize=1000000):
        for line in lines.iterrows():
            author = int(line[1][1])
            date = int(line[1][0])
            cnt += 1
            print(cnt)
            total_commits += 1
            if author not in authors:
                authors.append(author)

            #daily
            if author not in daily_contributors and date <= min_date + one_day:
                daily_contributors.append(author)
            if date <= min_date + one_day*daily_cnt:
                if author not in daily_temp_contrib:
                    daily_temp_contrib.append(author)
            else:
                daily_contributors = intersection(daily_contributors, daily_temp_contrib)
                daily_temp_contrib = []
                daily_cnt += 1
                while date > min_date + one_day*daily_cnt:
                    daily_commits.append(0)
                    daily_contributors = []
                    daily_cnt += 1
                daily_temp_contrib.append(author)
                daily_commits.append(1)
            
            #weekly
            if author not in weekly_contributors and date <= min_date + one_week:
                weekly_contributors.append(author)
            if date <= min_date + one_week*weekly_cnt:
                if author not in weekly_temp_contrib:
                    weekly_temp_contrib.append(author)
            else:
                weekly_contributors = intersection(weekly_contributors, weekly_temp_contrib)
                weekly_temp_contrib = []
                weekly_cnt += 1
                while date > min_date + one_week*weekly_cnt:
                    weekly_commits.append(0)
                    weekly_contributors = []
                    weekly_cnt += 1
                weekly_temp_contrib.append(author)
                weekly_commits.append(1)
            
            #monthly
            if author not in monthly_contributors and date <= min_date + one_month:
                monthly_contributors.append(author)
            if date <= min_date + one_month*monthly_cnt:
                if author not in monthly_temp_contrib:
                    monthly_temp_contrib.append(author)
            else:
                monthly_contributors = intersection(monthly_contributors, monthly_temp_contrib)
                monthly_temp_contrib = []
                monthly_cnt += 1
                while date > min_date + one_month*monthly_cnt:
                    monthly_commits.append(0)
                    monthly_contributors = []
                    monthly_cnt += 1
                monthly_temp_contrib.append(author)
                monthly_commits.append(1)

        #writing to file
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
        df.to_csv('/home/sv/project-git-metrics-first.csv', mode = 'a', header = False, index = False)
if __name__ == '__main__': 
    get_project_info()