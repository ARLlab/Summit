
import datetime as dt
import calendar
import numpy


def dateConv(arr):
    """
    An approach to convert decyear values into datetime values with numpy vectorization to improve efficiency

    :param arr: a numpy array of decyear values
    :return: a numpy array of datetime values
    """
    datetimes = []
    for i in range(len(arr)):

        year = int(arr[i])                                                  # get the year
        start = dt.datetime(year - 1, 12, 31)                               # create starting datetime
        numdays = (arr[i] - year) * (365 + calendar.isleap(year))           # calc number of days to current date
        result = start + dt.timedelta(days=numdays)                         # add timedelta of those days
        datetimes.append(result)                                            # append results

    return datetimes
