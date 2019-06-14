"""
This function plots the harmonic fitted data and residuals as timeseries for all the tracked NMHC by the GC-FID at
Summit, updated with 2019 data.
"""

# import libraries and functions
import pandas as pd
import numpy as np
from numba import njit
import numba as nb
import matplotlib.pyplot as plt
import seaborn as sns
from decToDatetime import convToDatetime
import datetime as dt




compounds = ['ethane', 'ethene', 'pentane', 'pentene', 'i-pentane', 'acetylene', 'n-pentane', 'i-butane', 'n_butane',
             'hexane', 'benzene', 'toulene']                                                # compound list

root = r'C:\Users\ARL\Desktop\J_Summit\analyses\HarmonicFit\textfiles'                      # data directory
header = ['yr', 'value', 'function', 'resid']                                               # dataframe headers

for cpd in compounds:
    filename = root + '\\' + cpd + 'FIT.txt'                                                # file ext
    data = pd.read_csv(filename, delim_whitespace=True, header=None,                        # data read
                       encoding='utf8', error_bad_lines=False)
    data.columns = header                                                                   # reset column names

    datetime = []
    for x in np.nditer(data['yr'].values):                                                  # conv decyear to datetime
        datetime.append(convToDatetime(0, x))
    data['datetime'] = datetime                                                             # assign to DF
    data.drop('yr', axis=1, inplace=True)                                                   # drop old column

    # trim outliers

    # plotting
    sns.set()                                                                               # setup
    f, ax = plt.subplots(ncols=2, figsize=(12, 8))                                          # 2 column subplot
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
    ax1.legend()

    # residual data
    ax3 = sns.scatterplot(x='datetime', y='resid', data=data, ax=ax[1],
                          alpha=1, s=10, legend='brief', label='Residuals from Fit')
    ax3.set_title('GC ' + cpd + ' Residuals from Fit')
    ax3.set_xlabel('Date')
    ax3.set_ylabel('Mixing Ratio [ppb]')
    ax3.legend()

    # save the plots
    direc = r'C:\Users\ARL\Desktop\J_Summit\analyses\Figures' + '\\' + cpd + '.png'
    f.savefig(direc, format='png')
    print(cpd + ' plots created')

