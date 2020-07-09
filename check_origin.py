import psycopg2
import pandas as pd
import os
from multiprocessing import Process
def check_origin(num):
    info(num)
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
        snapshot_cnt = {'0' : 519, '1': 1100, '2': 560, '3': 1200, '4': 521, '5': 1100}
        length = snapshot_cnt[num]
        while len(snapshots) < length:
            for lines in pd.read_csv('/mnt/17volume/data/snapshot_revision_fork_part0'+ num + '.csv', encoding='utf-8', header=None, chunksize=100):
                for line in lines.iterrows():
                    if cnt == 0:
                        cnt += 1
                        continue
#                     print(line)
                    try:
                        if line[1][0] not in snapshots:
                            check.clear()
                            snapshots.add(line[1][0])
                        if (line[1][0], line[1][2]) not in check:
                            row = cursor.execute("SELECT distinct ov1.snapshot_id as snapshot_id, ov2.snapshot_id as fork_id FROM origin_visit as ov1, origin_visit as ov2 WHERE (%s) = ov1.snapshot_id and (%s) = ov2.snapshot_id and ov1.origin != ov2.origin", (line[1][0], line[1][2]))
                            record = cursor.fetchone()
        #                     print(line[1][0], line[1][2])
                            if record != None:
                                check[(line[1][0], line[1][2])] = True
                            else:
                                check[(line[1][0], line[1][2])] = False
                        if check[(line[1][0], line[1][2])] == True:
                            df = pd.DataFrame({
                                'snapshot_id': [int(line[1][0])],
                                'fork_id': [int(line[1][2])],
                                'date': [int(line[1][3])]
                            })
                            df.to_csv('/home/sv/snapshot_fork_' + num + '.csv', mode ='a', header=False, index=False)
                    except Exception as error:
                        print(error)
                        f = open("/home/sv/check_origin_error.txt", "a")
                        f.write("Project id: " + str(line[1][0]))
                        f.write(", error: " + str(error) + "\n")
                        pass
    except psycopg2.Error as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        print(len(snapshots))
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
#     p1 = Process(target=check_origin, args = ('0',))
#     p1.start()
#     p2 = Process(target=check_origin, args = ('1',))
#     p2.start()
#     p3 = Process(target=check_origin, args = ('2',))
#     p3.start()
#    p4 = Process(target=check_origin, args = ('3',))
#    p4.start()
     p5 = Process(target=check_origin, args = ('4',))
     p5.start()
#     p6 = Process(target=check_origin, args = ('5',))
#     p6.start()
