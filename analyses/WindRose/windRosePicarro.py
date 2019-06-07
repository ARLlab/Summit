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
    pass

    # import data


if __name__ == '__main__':
    windRosePicarro()