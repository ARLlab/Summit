# import libraries and functions
from WindRose.metTrim import metTrim
import pandas as pd
import numpy as np


def metRemove(sheet, tolerance, dropMet=True):
    """
    metRemove is a function that removes values from a Summit dataset if the linked meteorlogical data is identified
    as being from a polluted zone. It takes a specific dataframe as an input, and returns the same exact dataframe.

    :param sheet: the dataframe can have any values but it must have a column labeled 'datetime'
    :param tolerance: the integer number of hours set to link met data with
    :param dropMet: default True, drops the columns of met data from the returned product. If false, met columns
    remain intact
    :return combo: returns the same dataframe, reindexed, polluted values removed
    """

    # identify and set the tolerence
    hours = f'{tolerance} hours'
    timedelt = pd.Timedelta(hours)

    # combine the input data with met data, dropping NaN's before and after
    met = metTrim()
    met.dropna(axis=0, how='any', inplace=True)
    sheet.dropna(axis=0, how='any', inplace=True)
    combo = pd.merge_asof(sheet.sort_values('datetime'), met,
                          on='datetime',
                          direction='nearest',
                          tolerance=timedelt)
    combo.dropna(axis=0, how='any', inplace=True)

    # remove polluted values
    speedCutoff = 1.02889               # meters/second, too slow
    dirCutoff = (342, 72)               # polution directions, above 342, below 72

    lenOrig = len(combo)

    combo = combo[combo['spd'] > speedCutoff]
    combo.reset_index(drop=True, inplace=True)

    valuesInRange = np.logical_or(combo['dir'] >= dirCutoff[0],
                                  combo['dir'] <= dirCutoff[1])

    combo.drop(combo[valuesInRange].index, axis=0, inplace=True)

    combo.reset_index(drop=True, inplace=True)

    percentChange = (1 - (len(combo) / lenOrig)) * 100
    print(f'{percentChange} percent of the data was trimmed from pollution identification')

    # remove unnecesary steady column
    badcol = ['steady']
    combo.drop(badcol, axis=1, inplace=True)

    if dropMet:
        combo.drop(['dir', 'spd'], axis=1, inplace=True)

    return combo

