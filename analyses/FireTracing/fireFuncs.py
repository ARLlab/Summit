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


def shapePlot():
    pass





