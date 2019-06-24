"""
This function plots the harmonic fitted data and residuals as timeseries for all the tracked NMHC by the GC-FID at
Summit, updated with 2019 data.
"""

# import libraries and functions

from dateConv import decToDatetime
from fileLoading import readCsv
from pandas.plotting import register_matplotlib_converters
from scipy import stats

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt

# main script

compounds = ['ethane', 'ethene', 'propane', 'propene', 'i_pentane', 'acetylene', 'n_pentane', 'i_butane', 'n_butane',
             'hexane', 'benzene', 'toulene']                                                # compound list

root = r'C:\Users\ARL\Desktop\J_Summit\analyses\Data'                                       # data directory
header = ['yr', 'value', 'function', 'resid']                                               # dataframe headers

register_matplotlib_converters()

for cpd in compounds:
    filename = root + '\\' + cpd + 'FIT.txt'                                                # file ext
    data = readCsv(filename)
    data.columns = header                                                                   # reset column names

    data = data[data['value'] > 0.0]

    normResid = data['resid'].values / data['value'].values
    data.drop(['resid'], axis=1, inplace=True)
    data['resid'] = normResid

    dates = decToDatetime(data['yr'].values)                                                # call conv function
    data['datetime'] = dates                                                                # assign to DF
    data.drop('yr', axis=1, inplace=True)

    # trim a few extreme outliers
    values = data['value'].values                                                           # get the value col
    z = np.abs(stats.zscore(values))                                                        # get the z score
    thresh = 2                                                                              # > 3 std devs
    data = data[~(z > thresh)]                                                              # boolean index

    resids = data['resid'].values                                                           # same thing but trim resid
    z = np.abs(stats.zscore(resids))
    thresh = 5
    data = data[~(z > thresh)]

    # y bounds
    mean = np.mean(values)
    lowV = min(data['value']) - (mean / 5)                                                 # arbitrary vals look ok
    highV = max(data['value']) + (mean / 5)

    mean = np.mean(data['resid'].values)
    lowR = min(data['resid']) - (mean / 5)
    highR = max(data['resid']) + (mean / 5)

    # x bounds
    low = min(data['datetime']) - dt.timedelta(days=30)
    high = max(data['datetime']) + dt.timedelta(days=30)

    # plotting
    sns.set()                                                                               # setup
    f, ax = plt.subplots(nrows=2, figsize=(12, 8))                                          # 2 column subplot
    sns.despine(f)
    plt.subplots_adjust(left=None, bottom=None, right=None,
                        top=None, wspace=0.3, hspace=0.5)

    # background data values with fitted harmonic functions
    ax1 = sns.scatterplot(x='datetime', y='value', data=data, ax=ax[0],
                          alpha=0.7, s=10, legend='brief', label='GC Data')
    ax2 = sns.lineplot(x='datetime', y='function', data=data, ax=ax[0], linewidth=2,
                       label='Fitted Curve')

    ax1.set_title('GC ' + cpd + ' Data with Fitted Function')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Mixing Ratio [ppb]')
    ax1.set(xlim=(low, high))
    ax1.set(ylim=(lowV, highV))
    ax1.get_lines()[0].set_color('#00b386')
    ax1.legend()

    # residual data
    ax3 = sns.scatterplot(x='datetime', y='resid', data=data, ax=ax[1],
                          alpha=1, s=10, legend='brief', label='Normalized Residuals from Fit')
    ax3.set_title('GC ' + cpd + ' Normalized Residuals from Fit')
    ax3.set_xlabel('Date')
    ax3.set_ylabel('Mixing Ratio [ppb]')
    ax3.legend()
    ax3.set(xlim=(low, high))
    ax3.set(ylim=(lowR, highR))

    # save the plots
    direc = r'C:\Users\ARL\Desktop\J_Summit\analyses\Figures' + '\\' + cpd + 'NORM.png'
    f.savefig(direc, format='png')
    print(cpd + ' plots created')

