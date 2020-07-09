import csv
from dateutil.parser import parse

class Revision:
    id = ""
    date = 1
    author = 1
    def __init__(self, date, author, id):
        self.date = date
        self.author = author
        self.id = id

def to_hour(date) :
    return (date*1.0) / (60*60*1000)
snapshots = {}

with open('snapshot_revision_other_bao.csv', mode = 'r') as project:
    csv_reader = csv.DictReader(project)
    line_count = 0
    for row in csv_reader:
        if row["snapshot_id"] not in snapshots:
            snapshots[row["snapshot_id"]] = []
            snapshots[row["snapshot_id"]].append(Revision(int(row["date"]), int(row["author"]), row["id"]))
        else:
            snapshots[row["snapshot_id"]].append(Revision(int(row["date"]), int(row["author"]), row["id"]))
        line_count+=1
    print(f'Processed {line_count} lines')


gap_limit = 96.0 # 4 days in hours
gap_limit_mili = 3.456e+8
with open('project-other-metrics.csv', mode='w') as metric:
    metric_writer = csv.writer(metric, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    metric_writer.writerow(['snapshot_id', 'total_revisions', 'total_authors', 'revision_frequency', 'author_frequency'])
    for id, snapshot in snapshots.items():
        snapshot.sort(key=lambda x: x.date)
        lower_date = 0
        higher_date = 0
        total_authors = 0
        authors = {}
        period_authors = {}
        total_commits = 0
        commit_frequency = 0
        period_cnt = 0
        author_frequency = 0
        for index, rev in enumerate(snapshot):
            total_commits += 1
            if rev.author not in authors:
                authors[rev.author] = 1
                total_authors+=1
            else:
                authors[rev.author] += 1
            commit_frequency += 1
            if lower_date == 0:
                lower_date = rev.date
            higher_date = rev.date
            if higher_date - lower_date <= gap_limit_mili and index == len(snapshot) - 1:
                period_cnt += 1
            if higher_date - lower_date > gap_limit_mili:
                lower_date = snapshot[index-1].date
                period_cnt += 1
                period_authors.clear()
            if rev.author not in period_authors:
                period_authors[rev.author] = 1
                author_frequency += 1
            else:
                period_authors[rev.author] += 1
        metric_writer.writerow([id, total_commits, total_authors, commit_frequency/period_cnt, author_frequency/ period_cnt])
