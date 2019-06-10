"""
Created on June 7th, 2019. The met data used here is courtesy of NOAA ESRL GMD. ftp://ftp.cmdl.noaa.gov/met/sum/README,
a full citation can be found in the file 'metTrim.py'. This function creates a windrose plot of cardinal direction
with with methane data from the GCFID and from the Picarro Analyzer

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
    met = metTrim()
    root = r'C:\Users\ARL\Desktop\J_Summit\analyses\HarmonicFit\textFiles'                      # root

    arl = pd.read_csv(root + r'\methaneARL.txt', encoding='utf8', delim_whitespace=True)        # import data
    colkeep = ['date', 'residuals']                                                             # col to keep
    arl = arl.drop(labels=(arl.columns[np.logical_and(arl.columns != colkeep[0],
                                                      arl.columns != colkeep[1])]),
                   axis=1)                                                                      # drop all other cols
    arl = arl.dropna(axis=0, how='any')                                                         # goodbye nan values

    # ---- combining datasets
    earlyVals = ~(met['DecYear'] <= arl['date'][0])                                             # early met vals
    met = met[earlyVals]                                                                        # remove those vals
    met = met.reset_index()                                                                     # reset index
    met = met.drop(['index', 'spd', 'steady'], axis=1)                                          # remove some columns

    metARL = pd.concat([met, arl], sort=False, axis=1)                                          # combine datasets
    dates, direc, gc = fastDateCombTwo(1, metARL['DecYear'].values,                                # call fast func
                                       metARL['date'].values,
                                       metARL['dir'].values,
                                       metARL['residuals'].values)
    metFinal = pd.DataFrame(columns=['DecYear', 'dir', 'gc_resid'])                # preallocate mat
    metFinal['DecYear'], metFinal['dir'], metFinal['gc_resid'] = dates, direc, gc               # append columns

    # metPicarro = pd.concat([met, picarroCH4], sort=False, axis=1)

    index = ~(metFinal <= 0)                                                                    # remove neg & zero
    metFinal = metFinal[index]
    metFinal = metFinal.dropna(axis=0, how='any')                                               # drop nan

    # ---- plotting
    fig, (ax1, ax2) = plt.subplots(1, 2, subplot_kw=dict(projection='windrose'))
    fig.suptitle('Methane Conc. Residuals by Wind Direction', fontsize=16)
    plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.2, hspace=None)

    # setup GC methane windrose
    ax1.bar(metFinal['dir'].values, metFinal['gc_resid'].values, normed=False, opening=0.9,
            edgecolor='black', nsector=24, bins=14, cmap=cm.viridis_r, blowto=False)
    ax1.set_title('GCFID Methane Conc. Residual [ppb]')
    ax1.set_legend(loc=6)

    plt.show()


if __name__ == '__main__':
    windRoseMethane()

