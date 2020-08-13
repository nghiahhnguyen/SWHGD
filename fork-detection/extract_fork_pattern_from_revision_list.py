#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
from datetime import datetime

# df = pd.read_csv("/home/sv/data/snapshot_fork_duplicates_removed_fixed.csv.gz", header=0, compression="gzip", names=["snapshot_id", "fork_id", "date"])
df = pd.read_csv("snapshot_fork_duplicates_removed_fixed.csv.gz", header=0, compression="gzip", names=["snapshot_id", "fork_id", "date"])

df['year'] = df.apply(lambda row: datetime.utcfromtimestamp(row['date']).year, axis=1)
df['month'] = df.apply(lambda row: datetime.utcfromtimestamp(row['date']).month, axis=1)
df['day'] = df.apply(lambda row: datetime.utcfromtimestamp(row['date']).day, axis=1)
df['week'] = df.apply(lambda row: datetime.utcfromtimestamp(row['date']).isocalendar()[1], axis=1)


snapshot_fork_dict = dict()
for i, row in df.iterrows():
    if row.snapshot_id not in snapshot_fork_dict:
        snapshot_fork_dict[row.snapshot_id] = dict()

    if row.year not in snapshot_fork_dict[row.snapshot_id]:
        # count of year, dictitionary for month, dictionary for week number
        snapshot_fork_dict[row.snapshot_id][row.year] = [1, dict(), dict()] 
    else:
        snapshot_fork_dict[row.snapshot_id][row.year][0] += 1
    
    if row.month not in snapshot_fork_dict[row.snapshot_id][row.year][1]:
        snapshot_fork_dict[row.snapshot_id][row.year][1][row.month] = 1
    else:
        snapshot_fork_dict[row.snapshot_id][row.year][1][row.month] += 1
    
    if row.week not in snapshot_fork_dict[row.snapshot_id][row.year][2]:
        snapshot_fork_dict[row.snapshot_id][row.year][2][row.week] = 1
    else:
        snapshot_fork_dict[row.snapshot_id][row.year][2][row.week] += 1

print("The number of project with forks:", len(snapshot_fork_dict))
for snapshot, revision_date_set in snapshot_fork_dict.items():
    print(snapshot, len(revision_date_set))

def count_time_units_exists(months, unit_per_year=12):
    units_exists = 0
    first_year, first_month = months[0]
    last_year, last_month = months[-1]
    if last_year == first_year:
        units_exists = last_month - first_month + 1
    elif last_month < first_month:
        extra_months = unit_per_year * (last_year - first_year) - 1
        units_exists = last_month + (12 - first_month + 1) + extra_months
    else:
        extra_months = unit_per_year * (last_year - first_year)
        units_exists = last_month - first_month + extra_months
    return units_exists

def is_steady(units, unit_per_year=12):
    year, unit = units[-1]
    num_iterations = unit_per_year
    while num_iterations:
        if (year, unit) not in units:
            return False
        unit -= 1
        if unit == 0:
            unit = unit_per_year
            year -= 1
        num_iterations -= 1
    return True

snapshot_ids = []
count_forks = []
count_years_exist = [] # the number of years between the first and last forks
count_months_exist = []
count_weeks_exist = []
mean_forks_per_year = []
mean_forks_per_month = []
mean_forks_per_week = []
count_years_with_forks = []
count_months_with_forks = []
count_week_with_forks = []
steady_month = []
steady_week = []
for snapshot, revision_date_set in snapshot_fork_dict.items():
    forks_year = []
    forks_month = []
    forks_week = []
    for year, (frequency, revision_date_set_month, revision_date_set_week) in revision_date_set.items():
        forks_year.append((year, frequency))
        for month, freq in revision_date_set_month.items():
            forks_month.append((year, month, freq))
        for week, freq in revision_date_set_week.items():
            forks_week.append((year, week, freq))
    forks_year.sort()
    forks_month.sort()
    forks_week.sort()
    count_fork = 0
    count_years_with_fork = 0
    count_months_with_fork = 0
    count_weeks_with_fork = 0
    years = []
    months = []
    weeks = []
    for year, frequency in forks_year:
        years.append(year)
        count_fork += frequency
        count_years_with_fork += 1
    for year, month, freq in forks_month:
        months.append((year, month))
        count_months_with_fork += 1
    for year, week, freq in forks_week:
        weeks.append((year, week))
        count_weeks_with_fork += 1
    
    months_exist = count_time_units_exists(months, 12)
    weeks_exist = count_time_units_exists(weeks, 53)
        
    snapshot_ids.append(snapshot)
    count_forks.append(count_fork)
    count_years_with_forks.append(count_years_with_fork)
    count_months_with_forks.append(count_months_with_fork)
    years_exist = years[-1] - years[0] + 1
    count_years_exist.append(years_exist)
    count_months_exist.append(months_exist)
    count_weeks_exist.append(weeks_exist)
    mean_forks_per_year.append(count_fork / years_exist)
    mean_forks_per_month.append(count_fork / months_exist)
    mean_forks_per_week.append(count_fork / weeks_exist)
    if is_steady(months, 12):
        steady_month.append(1)
    else:
        steady_month.append(0)
    if is_steady(weeks, 53):
        steady_week.append(1)
    else:
        steady_week.append(0)

df_fork_features = pd.DataFrame.from_dict({
    "snapshot_id": snapshot_ids,
    "count_forks": count_forks,
    "mean_forks_per_year": mean_forks_per_year,
    "mean_forks_per_month": mean_forks_per_month,
    "mean_forks_per_week": mean_forks_per_week,
    "count_years_with_forks": count_years_with_forks,
    "count_months_with_forks": count_months_with_forks,
    "count_years_with_forks": count_years_exist,
    "count_months_with_forks": count_months_exist,
    "count_weeks_with_fork": count_weeks_exist,
    "steady_month": steady_month,
    "steady_week": steady_week
    
}, orient='index').transpose()

print(df_fork_features.head(16))

df_fork_features.to_csv("/home/sv/data/fork_pattern.csv")

