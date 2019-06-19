"""
Created on June 7th, 2019. The met data used here is courtesy of NOAA ESRL GMD. ftp://ftp.cmdl.noaa.gov/met/sum/README,
a full citation can be found in the file 'metTrim.py'. This function creates a windrose plot of cardinal direction
with with CO and CO2 data from the Picarro analyzer at summit.

"""

# import libraries and functions
import matplotlib.pyplot as plt
from WindRose.metTrim import metTrim
import pandas as pd
import numpy as np
from numba import njit
import matplotlib.cm as cm


@njit
def fastDateCombTwo(tolerence, date1, date2, value1, value2):
    """
    fastDateCombThree takes values from a numpy array and combines data points with mutual dates (in decimal year)
    within a specified tolerence (in days). The final size of the array will be the size of the smaller array.

    note: output values that are negative or zero for the date are considered nan and can be removed. Comments for
    this function in more detail can be found in 'windRoseConc.py'

    :param tolerence: number of days where a value will be considered time equivalent with another
    :param date1: the decimal year dates from the LARGER dataset
    :param date2: the decimal year dates from the SMALLER dataset
    :param value1: the larger dataset values
    :param value2: the smaller dataset values
    :return: returned value arrays and a singular date array to be combined outside of function
    """
    rem = tolerence / 365
    dates = np.empty(date2.shape)
    value_1 = np.empty(value2.shape)
    value_2 = np.empty(value2.shape)

    for i in range(len(dates)):
        high = date2[i] + rem
        low = date2[i] - rem
        indices = (date1 <= high) & (date1 >= low)
        dirMed = np.nanmedian(value1[indices])
        if ~np.isnan(dirMed):
            dates[i] = date2[i]
            value_1[i] = dirMed
            value_2[i] = value2[i]

    return dates, value_1, value_2


def windRosePicarro():

    # ---- import data
    met = metTrim()
    root = r'C:\Users\ARL\Desktop\J_Summit\analyses\HarmonicFit\textFiles'

    co = pd.read_csv(root + r'\picarro_co.txt', delim_whitespace=True)
    co2 = pd.read_csv(root + r'\picarro_co2.txt', delim_whitespace=True)
    co.columns, co2.columns = ['date', 'value'], ['date', 'value']

    # ---- data adjustments
    earlyVals = ~(met['DecYear'] <= co['date'][0])                                               # trim early vals
    met = met[earlyVals]
    met = met.reset_index()
    met = met.drop(['index', 'spd', 'steady'], axis=1)                                              # drop cols

    lateVals = ~(co['date'] >= met['DecYear'].iloc[-1])
    co, co2 = co[lateVals], co2[lateVals]
    co, co2 = co.reset_index(), co2.reset_index()

    metCO = pd.concat([met, co], sort=False, axis=1)                                                # combine datasets
    dates, direc, gc = fastDateCombTwo(1, metCO['DecYear'].values,                                  # call fast func
                                       metCO['date'].values,
                                       metCO['dir'].values,
                                       metCO['value'].values)
    metCO_Final = pd.DataFrame(columns=['DecYear', 'dir', 'value'])                                 # preallocate mat
    metCO_Final['DecYear'], metCO_Final['dir'], metCO_Final['value'] = dates, direc, gc             # append columns

    index = ~(metCO_Final <= 0)                                                                     # remove neg & zero
    metCO_Final = metCO_Final[index]
    metCO_Final = metCO_Final.dropna(axis=0, how='any')                                             # drop nan

    metCO2 = pd.concat([met, co2], sort=False, axis=1)
    dates, direc, gc = fastDateCombTwo(1, metCO2['DecYear'].values,
                                       metCO2['date'].values,
                                       metCO2['dir'].values,
                                       metCO2['value'].values)
    metCO2_Final = pd.DataFrame(columns=['DecYear', 'dir', 'value'])
    metCO2_Final['DecYear'], metCO2_Final['dir'], metCO2_Final['value'] = dates, direc, gc

    index = ~(metCO2_Final <= 0)
    metCO2_Final = metCO2_Final[index]
    metCO2_Final = metCO2_Final.dropna(axis=0, how='any')

    # ---- plotting
    fig, (ax1, ax2) = plt.subplots(1, 2, subplot_kw=dict(projection='windrose'))
    fig.suptitle('CO & CO2 Conc. at Summit by Wind Direction', fontsize=16)
    plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.2, hspace=-0.2)

    # setup CO windrose
    ax1.bar(metCO_Final['dir'].values, metCO_Final['value'].values, normed=False, opening=0.9,
            edgecolor='black', nsector=24, bins=14, cmap=cm.viridis_r, blowto=False)
    ax1.set_title('CO Conc. [ppb]')
    ax1.set_legend(loc=8, fancybox=True, shadow=True, bbox_to_anchor=(0.5, -.5))

    # setup picarro CO2 windrose
    ax2.bar(metCO2_Final['dir'].values, metCO2_Final['value'].values, normed=False, opening=0.9,
            edgecolor='black', nsector=24, bins=14, cmap=cm.viridis_r, blowto=False)
    ax2.set_title('CO2 [ppm]')
    ax2.set_legend(loc=8, fancybox=True, shadow=True, bbox_to_anchor=(0.5, -.5))

    plt.show()


if __name__ == '__main__':
    windRosePicarro()