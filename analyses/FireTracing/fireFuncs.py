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
    dataframe.insert(len(dataframe.columns), 'yr', sep.str[0].astype(int), True)
    dataframe.insert(len(dataframe.columns), 'mo', sep.str[1].astype(int), True)
    dataframe.insert(len(dataframe.columns), 'dy', sep.str[2].astype(int), True)
    dataframe.insert(len(dataframe.columns), 'hr', np.zeros(len(dataframe['dy'])).astype(int), True)

    # create datetimes
    dataframe.insert(len(dataframe.columns), 'dt',
                     createDatetime(dataframe['yr'].values,
                                    dataframe['mo'].values,
                                    dataframe['dy'].values,
                                    dataframe['hr'].values))

    # add timedelta of hour and minute from the acq_time column
    timedeltas = pd.to_timedelta(timedeltas, unit='m')
    dataframe.insert(len(dataframe.columns), 'datetime',
                     dataframe['dt'] + timedeltas)

    # remove other columns
    badcols = ['acq_time', 'acq_date', 'yr', 'mo', 'dy', 'hr', 'dt']
    df = dataframe.drop(badcols, axis=1)

    return df


def fireCombo(fireDF, otherDF):
    import pandas as pd
    from fireFuncs import firedt

    # only keep high tolerence values
    cond = fireDF['confidence'] == 'h'
    fire = fireDF[cond]
    fire.reset_index(drop=True, inplace=True)

    # call datetime function to make datetimes
    fire = firedt(fire)

    # remove some other columns
    badcols = ['scan', 'track', 'satellite', 'instrument', 'confidence', 'version', 'type', 'frp']
    fire.drop(badcols, axis=1, inplace=True)
    fire.reset_index(drop=True, inplace=True)

    # identify Z scores of other DF

    # pd merge asof by datetime with the hour

    # print statement identifying how high z scores are? maybe the average z score?


    return fire

def shapePlot():
    pass





