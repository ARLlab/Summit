"""
Created on June 7th, 2019. The met data used here is courtesy of NOAA ESRL GMD. ftp://ftp.cmdl.noaa.gov/met/sum/README,
a full citation can be found in the file 'metTrim.py'. This function creates a windrose plot of cardinal direction
with various atmospheric tracer concentrations

"""

# import libraries and functions
import matplotlib.pyplot as plt
from WindRose.metTrim import metTrim
import pandas as pd
import numpy as np
from numba import njit
import matplotlib.cm as cm


@njit
def fastDateCombThree(tolerence, date1, date2, value1, value2, value3):
    """
    fastDateCombThree takes values from a numpy array and combines data points with mutual dates (in decimal year)
    within a specified tolerence (in days). The final size of the array will be the size of the smaller array. This
    specific case uses three values as optimized for the windRose dataset, but it can easily be changed.

    note: output values that are negative or zero for the date are considered nan and can be removed. The for loop
    method is faster than typical vectorizing because numba usings a C codebase

    :param tolerence: number of days where a value will be considered time equivalent with another
    :param date1: the decimal year dates from the LARGER dataset
    :param date2: the decimal year dates from the SMALLER dataset
    :param value1: the larger dataset values
    :param value2: the smaller dataset values
    :param value3: optional third dataset if it is same size as value2 (saves a step with indexing later)
    :return: returned value arrays and a singular date array to be combined outside of function
    """
    rem = tolerence / 365                                                       # tolerence day turned to dec
    dates = np.empty(date2.shape)                                               # preallocate arrays
    value_1 = np.empty(value2.shape)
    value_2 = np.empty(value2.shape)
    value_3 = np.empty(value3.shape)

    for i in range(len(dates)):                                                 # loop over each datapoint
        high = date2[i] + rem                                                   # upper date tolerence
        low = date2[i] - rem                                                    # lower date tolerence
        indices = (date1 <= high) & (date1 >= low)                              # larger data date points between tols
        dirMed = np.nanmedian(value1[indices])                                  # median of these indices
        if ~np.isnan(dirMed):                                                   # if median was not nan, update arrays
            dates[i] = date2[i]
            value_1[i] = dirMed
            value_2[i] = value2[i]
            value_3[i] = value3[i]

    return dates, value_1, value_2, value_3


def windRose():

    # ---- import data
    met = metTrim()
    root = r'C:\Users\ARL\Desktop\J_Summit\analyses\HarmonicFit\textfiles'  # root source
    ethane = pd.read_csv(root + r'\ethane.txt', encoding='utf8', delim_whitespace=True)
    ace = pd.read_csv(root + r'\acetylene.txt', encoding='utf8', delim_whitespace=True)

    # ---- trimming data
    titles_old = ['date', 'value', 'residuals']
    ace_titles_new = ['date_ace', 'resid_ace']
    eth_titles_new = ['date_eth', 'resid_eth']

    # drop unused columns for speed
    ethDrop = ethane.drop(labels=(ethane.columns[(np.logical_and(ethane.columns != titles_old[0],
                                                                 ethane.columns != titles_old[1]))]), axis=1)
    aceDrop = ace.drop(labels=(ace.columns[(np.logical_and(ace.columns != titles_old[0],
                                                           ace.columns != titles_old[1]))]), axis=1)

    ethDrop.columns = eth_titles_new  # rename columns
    aceDrop.columns = ace_titles_new

    earlyVals = ~(met['DecYear'] <= ethDrop['date_eth'][0])                     # early met vals
    met = met[earlyVals]                                                        # remove those vals
    met = met.reset_index()                                                     # reset index
    met = met.drop(['index', 'spd', 'steady'], axis=1)                          # remove some columns

    metEthane = pd.concat([met, ethDrop], sort=False, axis=1)                   # combine datasets
    metFull = pd.concat([metEthane, aceDrop], sort=False, axis=1)
    metFull['date_ace'] = metFull['date_eth']                                   # ethane and ace dates same, drop one
    metFull = metFull.drop(['date_ace'], axis=1)

    dates, met, ethane, ace = fastDateCombThree(1, metFull['DecYear'].values,   # call fast date combine function
                                                metFull['date_eth'].values,
                                                metFull['dir'].values,
                                                metFull['resid_eth'].values,
                                                metFull['resid_ace'].values)

    metFinal = pd.DataFrame(columns=['DecYear', 'dir', 'eth', 'ace'])           # preallocate DF
    # insert values into final dataframe
    metFinal['DecYear'], metFinal['dir'], metFinal['eth'], metFinal['ace'] = dates, met, ethane, ace

    index = ~(metFinal <= 0)                                                    # remove negative vals and 0
    metFinal = metFinal[index]
    metFinal = metFinal.dropna(axis=0, how='any')                               # drop nan

    # ---- plotting
    # setup subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, subplot_kw=dict(projection='windrose'))                            #
    fig.suptitle('NMHC Conc. Residuals by Wind Direction', fontsize=16)
    plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.2, hspace=0)

    # setup ethane windrose
    ax1.bar(metFinal['dir'].values, metFinal['eth'].values, normed=False, opening=0.9, edgecolor='black',
            nsector=24, bins=14, cmap=cm.viridis_r, blowto=False)
    ax1.set_title('Summit Ethane Conc. Residual [ppb]')
    ax1.set_legend(loc=6)

    # setup acetylene windrose
    ax2.bar(metFinal['dir'].values, metFinal['ace'].values, normed=False, opening=0.9, edgecolor='black',
            nsector=24, bins=6, cmap=cm.viridis_r, blowto=False)
    ax2.set_title('Summit Ace Conc. Residual [ppb]')
    ax2.set_legend(loc=7)

    plt.show()


if __name__ == '__main__':
    windRose()
