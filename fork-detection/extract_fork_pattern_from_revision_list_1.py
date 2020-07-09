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

def check_is_steady(units, unit_per_year=12):
    year, unit = units[-1]
    num_iterations = unit_per_year
    while num_iterations:
        num_iterations -= 1
        if (year, unit) not in units:
            return False
        unit -= 1
        if unit == 0:
            unit = unit_per_year
        year -= 1

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
    weeks_exist = count_time_units_exists(weeks, 52)
        
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
    "count_weeks_with_fork": count_weeks_exist
}, orient='index').transpose()

df_fork_features.head(16)