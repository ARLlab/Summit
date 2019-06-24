"""
Created on June 6th, 2019. The met data used here is courtesy of NOAA ESRL GMD. ftp://ftp.cmdl.noaa.gov/met/sum/README,
a full citation can be found in the file 'metTrim.py'. This function plots background Met Data for quality assurance

"""

# import libaries and functions
import matplotlib.pyplot as plt
import seaborn as sns
from WindRose.metTrim import metTrim
from pandas.plotting import register_matplotlib_converters
import datetime as dt
import matplotlib.cm as cm


def metPlotQC():

    register_matplotlib_converters()

    # import data
    met = metTrim()
    timelim = (met['datetime'].iloc[0] - dt.timedelta(days=20),
               met['datetime'].iloc[-1] + dt.timedelta(days=20))

    speedCutoff = 1.02889                           # meters/second, too slow
    dirCutoff = (342, 72)                           # polution directions, above 342, below 72

    sns.set()
    f, ax = plt.subplots(ncols=2, figsize=(12, 8))
    sns.despine(f)
    plt.subplots_adjust(left=None, bottom=None, right=None,
                        top=None, wspace=None, hspace=0.5)

    ax1 = sns.distplot(met['dir'], ax=ax[0])
    ax1.set_title('Summit Wind Direction Histogram')
    ax1.set_xlabel('Direction Clockwise from True North')
    ax1.set_ylabel('Value Distribution')
    # ax1.set(xlim=(timelim[0], timelim[1]))

    ax2 = sns.distplot(met['spd'], ax=ax[1])
    ax2.set_title('Summit Wind Speed Histogram')
    ax2.set_xlabel('Wind Speed [m/s]')
    ax2.set_ylabel('Value Distribution')
    # ax2.set(xlim=(timelim[0], timelim[1]))

    plt.show()

    sns.set(style="white")
    sns.despine()
    g = sns.jointplot(met['dir'], met['spd'], kind='hex', cmap=cm.magma_r, color='#e65c00')
    g.set_axis_labels('Wind Direction Measured CW from True North', 'Wind Speed [m/s]', fontsize=12)
    g.fig.suptitle('Met Data from Summit, Greenland')
    plt.legend()

    plt.show()


if __name__ == '__main__':
    metPlotQC()