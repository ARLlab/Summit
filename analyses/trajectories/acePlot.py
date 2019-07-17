"""
This script brings in acetylene data and uses the trajectory plot

"""
import numpy as np

from trajectoryPlotting import trajPlot
from fileLoading import readCsv
from dateConv import decToDatetime
from metRemove import metRemove
from scipy import stats

hours = 72
title = f'{hours}h Back Trajectories of Acetylene/Methane Ratio Outliers, 2012-2018'
root = r'C:\Users\ARL\Desktop\Jashan PySplit\pysplitprocessor-master\pysplitprocessor\ace_methane_traj'

dataroot = r'C:\Users\ARL\Desktop\Summit\analyses\Data'                     # data directory
ace = readCsv(dataroot + r'\aceRatioNoaa.txt')                                  # data read in acetylene

header = ['decyear', 'value', 'function', 'resid', 'residsmooth']           # assign column names
ace.columns = header
ace = ace[ace['value'] >= 0.00000001]

ace['datetime'] = decToDatetime(ace['decyear'].values)          # create datetimes from decyear

dates = ace['datetime'].tolist()  # put datetimes in a list
julian = []  # preallocate julian day list
for d in dates:  # loop over each date
    tt = d.timetuple()  # create a timetuple
    jul = tt.tm_yday  # identify julian day
    julian.append(jul)  # append to list
ace['julian'] = julian  # add to dataframe

cutoffs = (120, 305)                                                    # identify julian cutoffs
keep = np.logical_and(ace['julian'] >= cutoffs[0],                      # create boolean and array
                      ace['julian'] <= cutoffs[1])
ace = ace[keep]                                                         # boolean index to remove winter

dropcols = ['decyear', 'function', 'residsmooth']  # columns to drop
ace.drop(dropcols, axis=1, inplace=True)  # drop unused columns

# remove slow data or data above 342, below 72 degrees at Summit camp due to possible pollution
aceClean = metRemove(ace, 1, dropMet=True)

residuals = ace['resid'].values                                       # numpy array of resid
z = np.abs(stats.zscore(residuals))                                     # calculate z scores
ace['zscores'] = z                                                    # assign as column
thresh = 3                                                              # z score threshold
sheetZ = ace[z > thresh]                                              # remove non outliers

trajPlot(root, title=title, zscores=sheetZ)
