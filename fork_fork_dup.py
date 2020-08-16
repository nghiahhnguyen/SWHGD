import csv

url_to_ids = {}
origins = {}
# for lines in pd.read_csv('/home/sv/origin_url_data.csv', encoding='utf-8', header=None, chunksize=100000):
#         for line in lines.iterrows():
#             url_to_ids[line[1][1]] = int(line[1][0])
#             origins[int(line[1][0])] = [0, 0, 0]
# with open('/home/sv/origin_urls_data.csv', 'r') as f:
#         data = csv.reader(f)
#         for row in data:
#             if row[0] in url_to_ids and len(row) >= 4:
#                 if row[3] != '':
#                     origin_id = url_to_ids[row[0]]
#                     origins[origin_id][0] = int(row[3])

with open('/home/sv/fork_metrics_3.csv', 'r') as f:
    data = csv.reader(f)
    for row in data:
        origins[int(row[0])] = [int(row[1]), 0]
with open('/home/sv/dup_metrics_3.csv', 'r') as f:
    data = csv.reader(f)
    for row in data:
        origins[int(row[0])][1] = int(row[1])

with open('/home/sv/fork_dup_3.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["fork", "dup"])
    for origin in origins:
        writer.writerow(origins[origin])
