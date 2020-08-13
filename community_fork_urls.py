import psycopg2
import pandas as pd
import os
from multiprocessing import Process

def get_url_from_file(filename):
    info(filename)
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
        for lines in pd.read_csv('/mnt/17volume/data/community_fork.part.0' + filename, encoding='utf-8', header=None, chunksize=10000):
            for line in lines.iterrows():
                print('snapshot_id:',line[1][0])
                c = []
                c.append(int(line[1][0]))
                cursor.execute("SELECT o.url FROM origin_visit as ov join origin as o on ov.origin = o.id WHERE %s = ov.snapshot_id;", c)            
                record = cursor.fetchone()
                for rec in record:
                    print('url:', rec)
                print('first url:',record[0])
                # df = pd.DataFrame([int(line[1][0]), record[0]], columns=["snapshot_id","snapshot_url"])
                df = pd.DataFrame({
                    'snapshot_id' : [int(line[1][0])],
                    'snapshot_url' : [record[0]]
                })
                df.to_csv('/home/sv/community_fork_urls.csv', mode = 'a', header=False, index=False)
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())

if __name__ == '__main__':
    p1 = Process(target=get_url_from_file, args=('0'))
    p1.start()
    p2 = Process(target=get_url_from_file, args=('1'))
    p2.start()
    p3 = Process(target=get_url_from_file, args=('2'))
    p3.start()
    p4 = Process(target=get_url_from_file, args=('3'))
    p4.start()
    p5 = Process(target=get_url_from_file, args=('4'))
    p5.start()
    p6 = Process(target=get_url_from_file, args=('5'))
    p6.start()
    p7 = Process(target=get_url_from_file, args=('6'))
    p7.start()