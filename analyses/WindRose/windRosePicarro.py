"""
Created on June 7th, 2019. The met data used here is courtesy of NOAA ESRL GMD. ftp://ftp.cmdl.noaa.gov/met/sum/README,
a full citation can be found in the file 'metTrim.py'. This function creates a windrose plot of cardinal direction
with with CO and CO2 data from the Picarro analyzer at summit.

"""

# import libraries and functions
import matplotlib.pyplot as plt
from WindRose.metTrim import metTrim
import pandas as pd
from dateConv import decToDatetime
import windrose

import matplotlib.cm as cm


def picarroMetCombo(filename):

    met = metTrim()
    sheet = pd.read_csv(filename, encoding='utf8', header=None, delim_whitespace=True)
    sheet.columns = ['date', 'value']
    sheet['datetime'] = decToDatetime(sheet['date'].values)
    sheet.drop('date', axis=1, inplace=True)
    earlyVals = ~(met['datetime'] <= sheet['datetime'][0])
    met.drop(earlyVals, axis=0, inplace=True)
    met.reset_index(drop=True, inplace=True)
    met.drop(['steady'], axis=1, inplace=True)

    # merge the met data onto the concentration data by finding the nearest datetime within an hour\
    sheet.dropna(axis=0, how='any', inplace=True)
    picarro = pd.merge_asof(sheet.sort_values('datetime'), met,
                            on='datetime',
                            direction='nearest',
                            tolerance=pd.Timedelta('1 hour'))
    picarro.dropna(axis=0, how='any', inplace=True)

    return picarro


def windRosePicarro():

    # ---- import data
    root = r'C:\Users\ARL\Desktop\J_Summit\analyses\HarmonicFit\textFiles'

    co = picarroMetCombo(root + r'\picarro_co.txt')
    co2 = picarroMetCombo(root + r'\picarro_co2.txt')

    # ---- plotting
    fig, (ax1, ax2) = plt.subplots(1, 2, subplot_kw=dict(projection='windrose'))
    fig.suptitle('CO & CO2 Conc. at Summit by Wind Direction', fontsize=16)
    plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.2, hspace=-0.2)

    # setup CO windrose
    ax1.bar(co['dir'].values, co['value'].values, normed=False, opening=0.9,
            edgecolor='black', nsector=24, bins=14, cmap=cm.viridis_r, blowto=False)
    ax1.set_title('CO Conc. [ppb]')
    ax1.set_legend(loc=6, fancybox=True, shadow=True)

    # setup picarro CO2 windrose
    ax2.bar(co2['dir'].values, co2['value'].values, normed=False, opening=0.9,
            edgecolor='black', nsector=24, bins=14, cmap=cm.viridis_r, blowto=False)
    ax2.set_title('CO2 [ppm]')
    ax2.set_legend(loc=6, fancybox=True, shadow=True)

    plt.show()


if __name__ == '__main__':
    windRosePicarro()
