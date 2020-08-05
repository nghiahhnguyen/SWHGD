import csv
import gzip
from dateutil.parser import parse
from decimal import *
import pandas as pd
import gc
import os
from multiprocessing import Process
def calculate_cycle_times(name):
    info(name)
    origins = {}
    origin_id = 0
    url = ""
    for lines in pd.read_csv("/home/sv/origin_release_data.csv", chunksize=100000, encoding='utf-8', header=None):
        for line in lines.iterrows():
            try:
                if line[1][0] not in origins:
                    if len(origins) > 0:
                        print("Origin 1: ", origin_id)
                        origins[origin_id].sort()                            
                        tmp = []
                        res = 0.0
                        if len(origins[origin_id]) > 1:
                            for i in range(len(origins[origin_id])-1):
                                tmp.append(abs(int(origins[origin_id][i]) - int(origins[origin_id][i+1])))
                            m = sum(tmp)/len(tmp)
                            m = m / (60*60)
                            res = 1/float(m)
                        df = pd.DataFrame({
                            'origin_id':[origin_id], 
                            'url': [url],
                            'cycle_time':[res]
                        })
                        df.to_csv('/home/sv/cycle_time_release.csv', mode ='a', header=False, index=False)
                        del origins
                        gc.collect()
                    origins = {}
                    url = line[1][1]
                    origin_id = line[1][0]
                    if origin_id == origin_id:
                        datetime = parse(line[1][3])
                        origins[origin_id] = [datetime.timestamp()]
                else:
                    datetime = parse(line[1][3])
                    origins[origin_id].append(datetime.timestamp())
            except Exception as e:
                f = open("/home/sv/cycle_time_error.txt", "a")
                f.write("Project id: " + str(line[1][0]))
                f.write(", error: " + str(e) + "\n")
                with open('/home/sv/cycle_time_exception.csv', mode='a') as project_file:
                    project_writer = csv.writer(project_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    project_writer.writerow([line[1]])
                pass

    if len(origins) > 0:
        print("Origin: ", origin_id)
        origins[origin_id].sort()                            
        tmp = []
        res = 0.0
        if len(origins[origin_id]) > 1:
            for i in range(len(origins[origin_id])-1):
                tmp.append(abs(int(origins[origin_id][i]) - int(origins[origin_id][i+1])))
            m = sum(tmp)/len(tmp)
            m = m / (60*60)
            res = 1/float(m)
        else:
            res = 0
        df = pd.DataFrame({
            'origin_id':[origin_id], 
            'url': [url],
            'cycle_time':[res]
        })
        df.to_csv('/home/sv/cycle_time_release.csv', mode ='a', header=False, index=False)
    del origins
    gc.collect()
    
def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())
    
if __name__ == '__main__':
#     p1 = Process(target=calculate_cycle_times, args = ('aa',))
#     p1.start()
#     p2 = Process(target=calculate_cycle_times, args = ('ab',))
#     p2.start()
#     p3 = Process(target=calculate_cycle_times, args = ('ac',))
#     p3.start()  
#     p4 = Process(target=calculate_cycle_times, args = ('ad',))
#     p4.start()  
#     p5 = Process(target=calculate_cycle_times, args = ('ae',))
#     p5.start()
#     p6 = Process(target=calculate_cycle_times, args = ('af',))
#     p6.start()
    calculate_cycle_times('')

