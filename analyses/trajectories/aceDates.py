"""
This function identifies high z score values of the acetylene methane ratio and appropriates the dates into the same
format as the Pysplit Processor
"""
# import functions and libraries
from fileLoading import readCsv
from dateConv import decToDatetime
from metRemoval import metRemove
from scipy import stats

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns


def pysplitAcetylene():

    # import acetylene/methane ratio
    root = r'C:\Users\ARL\Desktop\Summit\analyses\Data'                     # data directory
    ace = readCsv(root + r'\aceRatioNoaa.txt')                              # data read in

    header = ['decyear', 'value', 'function', 'resid', 'residsmooth']       # assign column names
    ace.columns = header                                                    # reassign header

    print(f'Number of data points is {len(ace)}')
    ace = ace[ace['value'] >= 0.000000000001]                               # remove zero values
    print('Nonexistant and zero values removed')                            # print statement
    print(f'Number of data points is {len(ace)}')                           # length display

    ace['datetime'] = decToDatetime(ace['decyear'].values)                  # create datetimes from decyear

    dates = ace['datetime'].tolist()                                        # put datetimes in a list
    julian = []                                                             # preallocate julian day list
    for d in dates:                                                         # loop over each date
        tt = d.timetuple()                                                  # create a timetuple
        jul = tt.tm_yday                                                    # identify julian day
        julian.append(jul)                                                  # append to list
    ace['julian'] = julian                                                  # add to dataframe

    cutoffs = (120, 305)                                                    # identify julian cutoffs
    keep = np.logical_and(ace['julian'] >= cutoffs[0],                      # create boolean and array
                          ace['julian'] <= cutoffs[1])

    ace = ace[keep]                                                         # boolean index to remove winter days
    print('Data from Winter months removed')
    print(f'Number of data points is {len(ace)}')

    dropcols = ['decyear', 'function', 'residsmooth']                       # columns to drop
    ace.drop(dropcols, axis=1, inplace=True)                                # drop unused columns

    # remove slow data or data above 342, below 72 degrees at Summit camp due to possible pollution
    aceClean = metRemove(ace, 1, dropMet=True)
    print('Trimmed potentially polluted values based on met data')
    print(f'Number of data points is {len(aceClean)}')

    # plotting distibutions for z score identifiers
    sns.set()
    sns.distplot(aceClean['value'], label='Values')
    sns.distplot(aceClean['resid'], label='Residuals')
    plt.legend()
    plt.ylabel('Count')
    plt.xlabel('Ace/Methane Ratio Value')
    plt.title('Distribution of variables')

    residuals = aceClean['resid'].values                                    # numpy array of resid
    z = np.abs(stats.zscore(residuals))                                     # calculate z scores
    aceClean['zscores'] = z                                                 # assign as column
    thresh = 3                                                              # z score threshold
    aceZ = aceClean[z > thresh]                                             # remove non outliers
    print(f'Number of outliers is {len(aceZ)}')

    aceZ['dates'] = pd.to_datetime(aceZ['datetime'],                        # convert datetime column
                                   utc=True,                                # into UTC normalized timestamps
                                   infer_datetime_format=True)              # infer the format

    aceZ.drop('datetime', axis=1, inplace=True)                             # drop old datetimes
    aceZ.reset_index(drop=True, inplace=True)

    aceZ.to_csv((root + r'\acePysplit.txt'), sep=',', index=False)          # OUTPUT TO CSV FILE

    return aceZ


if __name__ == '__main__':
    pysplitAcetylene()

