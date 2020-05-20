import csv
import gzip
from dateutil.parser import parse
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
snapshots = {}
snapshot_id = ""
with gzip.open('/mnt/17volume/data/snapshot_revision_git.csv.gz', mode = 'r') as csv_reader_2:
    #csv_reader_2 = csv.reader(project_2)
    #next(csv_reader_2)
    csv_reader_2.readline()
    for l in csv_reader_2:
        l = l.decode("utf-8")
        #print(l)

        _row = [ x for x in l.split(",")]
        i = 0
        row = []
        while i < len(_row)-1:
            if i != 1:
                row.append(int(_row[i][1:-1]))
            else:
                row.append(_row[i][1:-1])
            i+=1
        #print(row)

        if row[0] not in snapshots:
            if len(snapshots) > 0:
                #print("ok")
                one_day = 86400
                one_week = 604800
                one_month = 2628000
                with gzip.open('/tmp/project-git-metrics.csv.gz', mode='a') as metric_file:
                    #metric_writer_2 = csv.writer(metric_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
#                     metric_writer_2.writerow(['snapshot_id', 'total_revisions', 'total_authors', 'daily_commits', 'daily_committed_contributors', 'weekly_commits', 'weekly_committed_contributors','monthly_commits', 'monthly_committed_contributors'])
#                     for id, snapshot in snapshots.items():
                    #print("something")
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
                        #print(snapshots[snapshot_id])
                        for _, rev in enumerate(snapshots[snapshot_id]):
                            #print(rev)

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
                #                     print("day")
                                    daily_contributors = []
                                    daily_idx += 1
                                daily_temp_contrib.append(rev.author)
                                daily_commits.append(1)

                            if rev.author not in weekly_contributors and rev.date <= weekly_wnd[0]:
                                weekly_contributors.append(rev.author)
                            if rev.date <= weekly_wnd[weekly_idx]:
                                if rev.author not in daily_temp_contrib:
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
                                if rev.author not in daily_temp_contrib:
                                    monthly_temp_contrib.append(rev.author)
                            else:
                                monthly_contributors = intersection(monthly_contributors, monthly_temp_contrib)
                                monthly_temp_contrib = []
                                monthly_idx += 1
                                while monthly_idx < len(monthly_wnd) and rev.date > monthly_wnd[monthly_idx]:
                                    monthly_commits.append(0)
                #                     print("month")
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
                        daily_freq = sum(daily_commits)/len(daily_commits)
                        weekly_freq = sum(weekly_commits)/len(weekly_commits)
                        monthly_freq = sum(monthly_commits)/len(monthly_commits)
                        #print(daily_freq)
                        line = str(snapshot_id) + "," + str(total_commits) + "," + str(daily_freq) + "," + str(len(daily_contributors)) + "," + str(weekly_freq) + "," + str(len(weekly_contributors)) + "," + str(monthly_freq) + "," + str(len(monthly_contributors))
                        #print(line)
                        line += "\n"
                        line = line.encode()
                        metric_file.write(line)
                       #metric_writer_2.writerow([snapshot_id, total_commits, len(authors), sum(daily_commits)/len(daily_commits), len(daily_contributors), sum(weekly_commits)/len(weekly_commits), len(weekly_contributors), sum(monthly_commits)/len(monthly_commits), len(monthly_contributors)])
                del snapshots
            snapshots = {}
            snapshot_id = row[0]
            snapshots[row[0]] = [Revision(row[2], row[3], row[1])]
        else:
            snapshots[row[0]].append(Revision(row[2], row[3], row[1]))
