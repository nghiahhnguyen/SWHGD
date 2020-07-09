import psycopg2
import pandas as pd
try:
    connection = psycopg2.connect(user = "sv",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "swhgd")

    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    #print ( connection.get_dsn_parameters(),"\n")
    check = {}
    snapshots = set()
    # Print PostgreSQL version
    #cursor.execute("SELECT version();")
    #record = cursor.fetchone()
    #print("You are connected to - ", record,"\n")
    cnt = 0
    for lines in pd.read_csv('/mnt/17volume/data/list_of_forks_apoc_subgraph_git_master_verified_6M_1.csv.gz', encoding='utf-8', header=None, chunksize=100):
        #print(lines)
        for line in lines.iterrows():
            if cnt == 0:
                cnt += 1
                continue
            if line[1][0] not in snapshots:
                check.clear()
                snapshots.add(line[1][0])
            if (line[1][0], line[1][2]) not in check:
                row = cursor.execute("SELECT distinct ov1.snapshot_id as snapshot_id, ov2.snapshot_id as fork_id FROM origin_visit as ov1, origin_visit as ov2 WHERE (%s) = ov1.snapshot_id and (%s) = ov2.snapshot_id and ov1.origin != ov2.origin", (line[1][0], line[1][2]))
                record = cursor.fetchone()
                print(line[1][0], line[1][2])
                if record != None:
                    check[(line[1][0], line[1][2])] = True
                else:
                    check[(line[1][0], line[1][2])] = False
            if check[(line[1][0], line[1][2])] == True:
                df = pd.DataFrame({
                    'snapshot_id': [line[1][0]],
                    'fork_id': [line[1][2]],
                    'date': [line[1][3]]
                })
                df.to_csv('/tmp/snapshot_fork.csv.gz', compression = 'gzip', mode ='a', header=False, index=False)
except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    print("number of projects: ", len(snapshots))
    #closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
