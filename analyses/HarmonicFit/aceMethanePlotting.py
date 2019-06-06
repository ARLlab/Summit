"""
Created on March 29th, 2019. This script works on preliminary plotting data from the CCGVU exported functions,
residuals, and other features in the Python environment.

This data (from test1.txt) is from CCGVU with the DEFAULT settings. This is three polynomial terms, and four
harmonic terms, with no switching in longterm or shortterm settings on the fast fourier transform
"""

# Import Libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt
import seaborn as sns


def acePlot():

    # Reading in File
    root = r'C:\Users\ARL\Desktop\J_Summit\analyses\HarmonicFit'
    data = pd.read_csv(root + r'\textFiles\aceDefault.txt', encoding='utf8', delim_whitespace=True)
    data = data.dropna(axis=1, how='any')

    data['date'] = data['date'] - 1900                  # weird date fix?

    # Exploratory Graphing
    sns.set()
    f, ax = plt.subplots(nrows=3)
    sns.despine(f)
    plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=0.6)  # adjust plot spacing
    ax1 = sns.scatterplot(x='date', y='value', data=data, alpha=0.7, label='Original Data', ax=ax[0])
    ax2 = sns.lineplot(x='date', y='function', data=data, alpha=0.8, linewidth=2, label='Fitted Function', ax=ax[0])
    ax3 = sns.lineplot(x='date', y='smooth', data=data, linewidth=1, label='Smoothed Fit', ax=ax[0])
    ax1.set_title('Acetylene / Methane Ratio')
    ax1.set_xlabel('Decimal Year')
    ax1.set_ylabel('Mixing Ratio [ppb]')
    ax1.get_lines()[0].set_color('purple')
    ax1.set(xlim=(2012, 2019))
    ax1.set(ylim=(-.0001, .0004))
    ax1.legend()

    ax4 = sns.scatterplot(x='date', y='residuals', data=data, alpha=0.7, label='Standard Residuals', ax=ax[1])
    ax5 = sns.lineplot(x='date', y='resid_smooth', data=data, alpha=0.8, linewidth=2, label='Residuals from Smoothed Fit',
                       ax=ax[1])
    ax6 = sns.lineplot(x='date', y='smooth_resid', data=data, linewidth=1, label='Smooth Residual Line', ax=ax[1])
    ax4.set_title('Fitted Function Residuals in Acetylene/Methane Ratio')
    ax4.set_xlabel('Decimal Year')
    ax4.set_ylabel('Mixing Ratio [ppb]')
    ax4.get_lines()[0].set_color('purple')
    ax4.set(xlim=(2012, 2019))
    ax4.set(ylim=(-.00012, .0002))
    ax4.legend()

    # Day of Year Plot of Residuals
    doy = []                                                                        # Preallocate DOY List
    for x in data['date']:
        start = x                                                                   # current date
        year = int(start)                                                           # Year of current date
        rem = start - year                                                          # reminder decimal (mo/day)
        base = dt.datetime(year, 1, 1)                                              # Base

        # Convert the current x value in the day by adding the timedelta of reminder in seconds
        result = base + dt.timedelta(seconds=(base.replace(year=base.year + 1) - base).total_seconds() * rem)

        julianDay = result.timetuple().tm_yday                                      # convert to Julian day
        doy.append(julianDay)                                                       # append the results

    data.insert(1, 'DOY', doy)                                                      # insert into main datafrane

    ax7 = sns.scatterplot(x='DOY', y='residuals', data=data, alpha=0.7, label='Standard Residuals', ax=ax[2])
    ax8 = sns.scatterplot(x='DOY', y='resid_smooth', data=data, alpha=0.2, label='Residuals from Smoothed Fit', ax=ax[2])
    ax7.set_title('Daily Residuals in Acetylene/Methane Ratio')
    ax7.set_xlabel('Decimal Year')
    ax7.set_ylabel('Mixing Ratio [ppb]')
    ax7.set(ylim=(-.00015, .00025))
    ax7.set(xlim=(-5, 370))
    ax7.legend()

    plt.show()

    return data


if __name__ == '__main__':
    acePlot()
