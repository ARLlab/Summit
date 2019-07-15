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


def fireCombo(fireDF, otherDF, VIRRS=True):
    from fireFuncs import firedt
    import numpy as np
    from scipy import stats
    import pandas as pd

    if VIRRS:
        # only keep high tolerence values for VIRRS
        cond = fireDF['confidence'] != 'l'
        fireDF = fireDF[cond]
        fireDF.reset_index(drop=True, inplace=True)

    # call datetime function to make datetimes
    fireDF = firedt(fireDF)

    if VIRRS:
        # remove some other columns for VIRRS Version
        badcols = ['scan', 'track', 'satellite', 'instrument', 'confidence', 'version', 'type', 'frp']
        fireDF.drop(badcols, axis=1, inplace=True)
        fireDF.reset_index(drop=True, inplace=True)
    else:
        badcols = ['scan', 'track', 'satellite', 'instrument', 'confidence', 'version', 'type', 'frp', 'daynight']
        fireDF.drop(badcols, axis=1, inplace=True)
        fireDF.reset_index(drop=True, inplace=True)

    # identify Z scores of other DF in value and normed Resid
    values = otherDF['value'].values
    z = np.abs(stats.zscore(values))
    otherDF['value_z'] = z

    normedvals = otherDF['normResid'].values
    z = np.abs(stats.zscore(normedvals))
    otherDF['normed_z'] = z

    # pd merge asof by datetime with the hour
    combo = pd.merge_asof(fireDF.sort_values('datetime'), otherDF,
                          on='datetime',
                          direction='nearest',
                          tolerance=pd.Timedelta('1 hours'))
    combo.dropna(axis=0, how='any', inplace=True)
    combo.reset_index(drop=True, inplace=True)

    # remove some other columns
    badcols = ['decyear', 'function', 'residsmooth']
    combo.drop(badcols, axis=1, inplace=True)

    return combo


def shapePlot():
    pass





