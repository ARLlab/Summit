"""
Created on June 7th, 2019. The met data used here is courtesy of NOAA ESRL GMD. ftp://ftp.cmdl.noaa.gov/met/sum/README,
a full citation can be found in the file 'metTrim.py'. This function creates a windrose plot of cardinal direction
with with methane data from the GCFID and from the Picarro Analyzer

This function uses raw methane data points and not the residuals from the NOAA GMD CCG Harmonic fitting tool. This is
because the short timespan of the picarro data results in a poor harmonic fit. Reanalysis should be done once a strong
seasonal cycle has been developed in the data.

"""

# import libraries and functions
import matplotlib.pyplot as plt
from WindRose.metTrim import metTrim
import pandas as pd
import numpy as np
from numba import njit
import matplotlib.cm as cm
import windrose


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


def windRoseMethane():
    pass

    # ---- import data
    met = metTrim()                                                                             # met data
    root = r'C:\Users\ARL\Desktop\J_Summit\analyses\HarmonicFit\textFiles'                      # root

    arl = pd.read_csv(root + r'\methaneARL.txt', encoding='utf8', delim_whitespace=True)        # GC methane data
    pic = pd.read_csv(root + r'\picarro_ch4.txt', delim_whitespace=True, encoding='utf8')       # picarro ch4 data
    pic.columns = ['date', 'value']
    colkeep = ['date', 'value']                                                                 # col to keep
    arl = arl.drop(labels=(arl.columns[np.logical_and(arl.columns != colkeep[0],
                                                      arl.columns != colkeep[1])]),
               axis=1)                                                                      # drop all other cols
    arl = arl.dropna(axis=0, how='any')                                                         # goodbye nan values

    # ---- combining datasets (met and GC)
    earlyVals = ~(met['DecYear'] <= arl['date'][0])                                             # early met vals
    met = met[earlyVals]                                                                        # remove those vals
    met = met.reset_index()                                                                     # reset index
    met = met.drop(['index', 'spd', 'steady'], axis=1)                                          # remove some columns

    metARL = pd.concat([met, arl], sort=False, axis=1)                                          # combine datasets
    dates, direc, gc = fastDateCombTwo(1, metARL['DecYear'].values,                             # call fast func
                                       metARL['date'].values,
                                       metARL['dir'].values,
                                       metARL['value'].values)
    metFinal = pd.DataFrame(columns=['DecYear', 'dir', 'value'])                                # preallocate mat
    metFinal['DecYear'], metFinal['dir'], metFinal['value'] = dates, direc, gc                  # append columns

    index = ~(metFinal <= 0)                                                                    # remove neg & zero
    metFinal = metFinal[index]
    metFinal = metFinal.dropna(axis=0, how='any')                                               # drop nan

    # ---- combining datasets (met and picarro)
    earlyVals = ~(met['DecYear'] <= pic['date'][0])                                             # early date values
    met = met[earlyVals]                                                                        # remove
    met = met.reset_index()

    lateVals = ~(pic['date'] >= met['DecYear'].iloc[-1] )                        # late date value
    pic = pic[lateVals]                                                                         # removal
    pic = pic.reset_index()

    metPic = pd.concat([met, pic], sort=False, axis=1)
    dates, direc, gc = fastDateCombTwo(1, metPic['DecYear'].values,                             # call fast func
                                       metPic['date'].values,
                                       metPic['dir'].values,
                                       metPic['value'].values)

    metFinal2 = pd.DataFrame(columns=['DecYear', 'dir', 'value'])                               # preallocate mat
    metFinal2['DecYear'], metFinal2['dir'], metFinal2['value'] = dates, direc, gc               # append columns

    index = ~(metFinal2 <= 0)                                                                   # remove neg & zero
    metFinal2 = metFinal2[index]
    metFinal2 = metFinal2.dropna(axis=0, how='any')                                             # drop nan
    metFinal2 = metFinal2.reset_index()

    # ---- plotting
    fig, (ax1, ax2) = plt.subplots(1, 2, subplot_kw=dict(projection='windrose'))
    fig.suptitle('Methane Conc. at Summit by Wind Direction', fontsize=16)
    plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.2, hspace=None)

    # setup GC methane windrose
    ax1.bar(metFinal['dir'].values, metFinal['value'].values, normed=False, opening=0.9,
            edgecolor='black', nsector=24, bins=14, cmap=cm.viridis_r, blowto=False)
    ax1.set_title('GCFID Methane Conc. [ppb]')
    ax1.set_legend(loc=6)

    # setup picarro methane windrose
    ax2.bar(metFinal2['dir'].values, metFinal2['value'].values, normed=False, opening=0.9,
            edgecolor='black', nsector=24, bins=14, cmap=cm.viridis_r, blowto=False)
    ax2.set_title('Picarro Methane Conc. [ppb]')
    ax2.set_legend(loc=7)

    plt.show()


if __name__ == '__main__':
    windRoseMethane()

