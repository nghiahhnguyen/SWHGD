import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb

projectGitMetrics = pd.read_csv("project-git-metrics.csv")
projectOtherMetrics = pd.read_csv("project-other-metrics.csv")
projectCycleTime = pd.read_csv("snapshot_cycle_time.csv")

projectMetrics = pd.concat([projectGitMetrics, projectOtherMetrics]).drop_duplicates()

table = pd.merge(projectMetrics, projectCycleTime, on='snapshot_id', how='outer')

rawData1 = pd.read_csv("snapshot_revision_git_bao.csv")
rawData2 = pd.read_csv("snapshot_revision_other_bao.csv")

rawData1.info()
rawData2.info()

rawData = pd.concat([rawData1, rawData2])

rawData.info()

table = rawData.groupby("date").agg(['first', 'last']).stack()

print(table)