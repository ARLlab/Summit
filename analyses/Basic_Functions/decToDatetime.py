"""
This custom function turns decimal date of years into a datetime format.
:param: year: the year for the datetime
:param: decyear: the decimal year. Can be either of the form year.xxxxx (ex: 2019.7283)

                 or of the form day.xxx (ex: 3.394)
"""
import datetime as dt
from isleapyear import isleapyear

def convToDatetime(year, decyear):

    # get the starting point from inputted year
    start = dt.datetime(year - 1, 12, 31)

    # if datetime is entered with the year, subtract the year to get just the decimal and turn into days format
    if decyear > 2000:
        numdays = (decyear - year) * (365 + isleapyear(year))
    # otherwise, entered decyear was the number of days
    else:
        numdays = decyear

    # return the timedelta of the days added to start
    result = start + dt.timedelta(days=numdays)

    return result