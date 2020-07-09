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
snapshots = {}
with open('snapshot_revision_git_bao.csv', mode = 'r') as project_2:
    csv_reader_2 = csv.DictReader(project_2)
    line_count = 0
    for row in csv_reader_2:
        if row["snapshot_id"] not in snapshots:
            snapshots[row["snapshot_id"]] = []
            snapshots[row["snapshot_id"]].append(Revision(int(row["date"]), int(row["author"]), row["id"]))
        else:
            snapshots[row["snapshot_id"]].append(Revision(int(row["date"]), int(row["author"]), row["id"]))
        line_count+=1
    print(f'Processed {line_count} lines')
gap_limit_mili = 3.456e+8
with open('project-git-metrics.csv', mode='w') as metric_file:
    metric_writer_2 = csv.writer(metric_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    metric_writer_2.writerow(['snapshot_id', 'total_revisions', 'total_authors', 'revision_frequency', 'author_frequency'])
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
        metric_writer_2.writerow([id, total_commits, total_authors, commit_frequency/period_cnt, author_frequency/ period_cnt])

