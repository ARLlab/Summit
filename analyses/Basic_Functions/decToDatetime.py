"""
This custom function turns decimal date of years into a datetime format. If decyear is in the form with the year,
entire the year parameter as anything, it will be overwritten
:param: decyear: the decimal year. Should be of the form year.xxxxx (ex: 2019.7283)
"""
import datetime as dt
from isleapyear import isleapyear
import numpy as np


def convToDatetime(decyear):

    # if the decyear is nan, just return it as a nan
    if np.isnan(decyear):
        return decyear

    else:
        # get the starting point from inputted year
        year = int((str(decyear))[:4])
        start = dt.datetime(year - 1, 12, 31)
        numdays = (decyear - year) * (365 + isleapyear(year))

        # return the timedelta of the days added to start
        result = start + dt.timedelta(days=numdays)

        return result
