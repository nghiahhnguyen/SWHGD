import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb

projectGitMetrics = pd.read_csv("project-git-metrics.csv")
projectOtherMetrics = pd.read_csv("project-other-metrics.csv")
projectCycleTime = pd.read_csv("snapshot_cycle_time.csv")

projectMetrics = pd.concat([projectGitMetrics, projectOtherMetrics]).drop_duplicates()

# table = pd.merge(projectMetrics, projectCycleTime, on='snapshot_id', how='outer')

# longevity
rawData1 = pd.read_csv("snapshot_revision_git_bao.csv")
rawData2 = pd.read_csv("snapshot_revision_other_bao.csv")
rawData = pd.concat([rawData1, rawData2])

table = rawData.sort_values(['snapshot_id', 'date'], ascending=[True, True])

table2 = table[['snapshot_id','date']]

table3 = table2.groupby(['snapshot_id'], as_index=True)['date'].agg(['first', 'last']).reset_index()

table3['longevity'] = table3['last'] - table3['first']

# table3['longevity'] = pd.to_timedelta(table3['longevity'], unit='ms')

longevityMetric = table3[['snapshot_id','longevity']]

longevityMetric.to_csv("longevity_metrics.csv", encoding='utf-8', index=False)

projectMetrics = pd.merge(projectMetrics, longevityMetric, on='snapshot_id', how='outer')

projectMetrics.info()

pearsonCorr = projectMetrics.corr(method='pearson')

pearsonCorr.to_csv("pearson_correlation_coefficient.csv", encoding='utf-8', index=True)

print(pearsonCorr)

sbplot = sb.heatmap(pearsonCorr, 
            xticklabels=pearsonCorr.columns,
            yticklabels=pearsonCorr.columns,
            cmap='RdBu_r',
            annot=True,
            linewidth=1)

sbplot.get_figure().savefig("corr.png")