import psycopg2
import pandas as pd

snapshot_urls = []
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
    for lines in pd.read_csv('/mnt/17volume/data/community_fork.csv', encoding='utf-8', header=None, chunksize=100000):
        for line in lines.iterrows():
            print('snapshot_id:',line[1][0])
            c = []
            c.append(int(line[1][0]))
            cursor.execute("SELECT o.url FROM origin_visit as ov join origin as o on ov.origin = o.id WHERE %s = ov.snapshot_id;", c)
            record = cursor.fetchone()
            print('url:',record[0])
            snapshot_urls.append(record[0])
except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
    df = pd.DataFrame(snapshot_urls, columns=["snapshot_urls"])
    df.to_csv('/home/sv/snapshot_urls.csv', index=False)
