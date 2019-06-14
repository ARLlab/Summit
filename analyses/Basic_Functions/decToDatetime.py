"""
This custom function turns decimal date of years into a datetime format. If decyear is in the form with the year,
entire the year parameter as anything, it will be overwritten
:param: year: the year for the datetime
:param: decyear: the decimal year. Can be either of the form year.xxxxx (ex: 2019.7283) or of the form day.xxx (ex:
3.394)
"""
import datetime as dt
from isleapyear import isleapyear
import numpy as np


def convToDatetime(year, decyear):

    # if decyear is in the form of year.xxx, extract the year value
    if np.any(decyear > 2000):
        year = int((str(decyear))[:4])

    # if the decyear is nan, just return it as a nan
    if np.isnan(decyear):
        return decyear

    else:
        # get the starting point from inputted year
        start = dt.datetime(year - 1, 12, 31)

        # if datetime is entered with the year, subtract the year to get just the decimal and turn into days format
        if decyear > 2000:
            numdays = (decyear - year) * (365 + isleapyear(year))
        # otherwise, entered decyear was the number of days
        else:
            numdays = decyear

        # return the timedelta of the days added to start, remove the microseconds
        result = start + dt.timedelta(days=numdays)

        return result
