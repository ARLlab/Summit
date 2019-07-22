"""
This function identifies high z score values of the acetylene methane ratio, and the ethane methane ratio and
appropriates the dates into the same format as the Pysplit Processor
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


def pysplitDates():

    # import ratio
    root = r'C:\Users\ARL\Desktop\Summit\analyses\Data'                     # data directory
    ethane = readCsv(root + r'\ethaneRatioNoaa.txt')                        # ethane data read in
    ace = readCsv(root + r'\aceRatioNoaa.txt')                              # data read in acetylene

    for sheet in [ethane, ace]:
        header = ['decyear', 'value', 'function', 'resid', 'residsmooth']       # assign column names
        sheet.columns = header
        print(f'Number of data points is {len(sheet)}')

    ethane = ethane[ethane['value'] >= 0.0000001]
    ace = ace[ace['value'] >= 0.00000001]

    for sheet in [ethane, ace]:
        print('Nonexistant and zero values removed')                        # print statement
        print(f'Number of data points is {len(sheet)}')                     # length display

        sheet['datetime'] = decToDatetime(sheet['decyear'].values)          # create datetimes from decyear

        dates = sheet['datetime'].tolist()                                  # put datetimes in a list
        julian = []                                                         # preallocate julian day list
        for d in dates:                                                     # loop over each date
            tt = d.timetuple()                                              # create a timetuple
            jul = tt.tm_yday                                                # identify julian day
            julian.append(jul)                                              # append to list
        sheet['julian'] = julian                                            # add to dataframe

        dropcols = ['decyear', 'function', 'residsmooth']  # columns to drop
        sheet.drop(dropcols, axis=1, inplace=True)  # drop unused columns

    cutoffs = (120, 305)                                                    # identify julian cutoffs
    keep = np.logical_and(ace['julian'] >= cutoffs[0],                      # create boolean and array
                          ace['julian'] <= cutoffs[1])
    keep = np.logical_and(ethane['julian'] >= cutoffs[0],                   # create boolean and array
                          ethane['julian'] <= cutoffs[1])

    ace = ace[keep]                                                         # boolean index to remove winter
    ethane = ethane[keep]

    # remove slow data or data above 342, below 72 degrees at Summit camp due to possible pollution
    aceClean = metRemove(ace, 1, dropMet=True)
    ethaneClean = metRemove(ethane, 1, dropMet=True)
    print('Trimmed potentially polluted values based on met data')
    print(f'Number of data points is {len(aceClean)}')
    print(f'Number of data points is {len(ethaneClean)}')

    # plotting distibutions for z score identifiers
    sns.set()
    sns.distplot(aceClean['value'], label='Values')
    sns.distplot(aceClean['resid'], label='Residuals')
    plt.legend()
    plt.ylabel('Count')
    plt.xlabel('Ace/Methane Ratio Value')
    plt.title('Distribution of variables')

    sns.set()
    sns.distplot(ethaneClean['value'], label='Values')
    sns.distplot(ethaneClean['resid'], label='Residuals')
    plt.legend()
    plt.ylabel('Count')
    plt.xlabel('Ethane/Methane Ratio Value')
    plt.title('Distribution of variables')

    for sheet in [aceClean, ethaneClean]:
        residuals = sheet['resid'].values                                       # numpy array of resid
        z = np.abs(stats.zscore(residuals))                                     # calculate z scores
        sheet['zscores'] = z                                                    # assign as column
        thresh = 0                                                              # z score threshold
        sheetZ = sheet[z > thresh]                                              # remove non outliers
        print(f'Number of outliers is {len(sheetZ)}')

        sheetZ['dates'] = pd.to_datetime(sheetZ['datetime'],                    # convert datetime column
                                         utc=True,                              # into UTC normalized timestamps
                                         infer_datetime_format=True)            # infer the format

        sheetZ.drop('datetime', axis=1, inplace=True)                           # drop old datetimes
        sheetZ.reset_index(drop=True, inplace=True)

        # OUTPUT TO CSV FILE
        if sheet.iloc[0][0] == aceClean.iloc[0][0]:
            sheetZ.to_csv((root + r'\acePysplit.txt'), sep=',', index=False)
        else:
            sheetZ.to_csv((root + r'\ethanePysplit.txt'), sep=',', index=False)


if __name__ == '__main__':
    pysplitDates()

