"""
General functions for fire tracing
"""


def readSF(filename):
    """
    Read a shape file and convert it into a pandas dataframe

    :param filename: the filename with its full location on the computer
    :return: a pandas dataframe
    """
    import shapefile as shp
    import pandas as pd

    sf = shp.Reader(filename)                               # read shapefile
    titles = [x[0] for x in sf.fields][1:]                  # get col names
    records = [list(x) for x in sf.records()]               # get records
    fire = pd.DataFrame(columns=titles, data=records)       # create DF

    return fire, sf


def firedt(dataframe):
    """
    :param dataframe: a dataframe of NASA VIIRS data, with their column titles
    :return: the same dataframe with a datetime column
    """

    import datetime as dt
    import pandas as pd
    import numpy as np
    from dateConv import createDatetime

    # preallocate and define used columns of dataframe
    values = dataframe['acq_date']
    timedeltas = dataframe['acq_time']

    # seperate datetime components
    sep = values.str.split('-')
    dataframe['yr'] = sep.str[0].astype(int)
    dataframe['mo'] = sep.str[1].astype(int)
    dataframe['dy'] = sep.str[2].astype(int)
    dataframe['hr'] = np.zeros(len(dataframe['dy'])).astype(int)

    # create datetimes
    dataframe['datetime'] = createDatetime(dataframe['yr'].values,
                                           dataframe['mo'].values,
                                           dataframe['dy'].values,
                                           dataframe['hr'].values)

    # add timedelta of hour and minute from the acq_time column
    timedeltas = pd.to_timedelta(timedeltas, unit='m')
    dataframe['datetime'] = dataframe['datetime'] + timedeltas

    # remove other columns
    badcols = ['acq_time', 'acq_date', 'yr', 'mo', 'dy', 'hr']
    dataframe.drop(badcols, axis=1, inplace=True)

    return dataframe


def fireCombo(fireDF, otherDF):
    import pandas as pd
    from fireFuncs import firedt

    root = r'C:\Users\'
    fire = pd.read_csv(root + r'\FireData' + r'\fire_archive_V1_58066.csv')

    # only keep high tolerence values
    cond = fire['confidence'] == 'h'
    fire = fire[cond]
    fire.reset_index(drop=True, inplace=True)

    # call datetime function to make datetimes
    fire = firedt(fire)

    # remove some other columns
    badcols = ['scan', 'track', 'satellite', 'instrument', 'confidence', 'version', 'type', 'frp']
    fire.drop(badcols, axis=1, inplace=True)
    fire.reset_index(drop=True, inplace=True)
    pass


def shapePlot():
    pass





