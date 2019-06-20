"""
dateConv is a centralized script that contains a variety of functions useful for quickly and rapidly converting
datetimes. This is a common issue to a large number of datasets. The functions in here are used for analyses across
the entire root folder.

"""


def toYearFraction(date):
    """

    :param date: This function takes in a singular datetime value
    :return: returns a decimal year float value
    """
    from datetime import datetime as dt
    import time

    # returns seconds since epoch
    def sinceEpoch(datetime):
        return time.mktime(datetime.timetuple())
    s = sinceEpoch

    year = date.year
    startOfThisYear = dt(year=year, month=1, day=1)
    startOfNextYear = dt(year=year+1, month=1, day=1)

    yearElapsed = s(date) - s(startOfThisYear)
    yearDuration = s(startOfNextYear) - s(startOfThisYear)
    fraction = yearElapsed/yearDuration

    return date.year + fraction


def isleapyear(yr):
    """

    :param yr: an integer year value (i.e: 2019)
    :return: boolean, True if a leap year, False if not a leap year
    """
    import pandas as pd

    # Month and Day do not matter, just required. Converts to dataframe
    placeholder = pd.DataFrame({'year': [yr], 'month': [1], 'day': [1]})

    # Converts to the datetime format
    date = pd.to_datetime(placeholder)

    # Pandas function to tell if leap year
    leap = int(date.dt.is_leap_year)

    return leap


def decToDatetime(arr):
    """
    An approach to convert decyear values into datetime values with numpy vectorization to improve efficiency

    :param arr: a numpy array of decyear values
    :return: a numpy array of datetime values
    """

    import datetime as dt
    import calendar

    datetimes = []
    for i in range(len(arr)):

        year = int(arr[i])                                                  # get the year
        start = dt.datetime(year - 1, 12, 31)                               # create starting datetime
        numdays = (arr[i] - year) * (365 + calendar.isleap(year))           # calc number of days to current date
        result = start + dt.timedelta(days=numdays)                         # add timedelta of those days
        datetimes.append(result)                                            # append results

    return datetimes


def noaaDateConv(dataframe):
    """
    This function takes a dataframe with datetime values and converts it into a format that the NOAA ccg tool can
    easily read

    :param dataframe: A dataframe that has to have a column labeled 'datetime' which contains dt.datetime formatted
    items
    :return: the same dataframe with the datetime column replaced by year, month, day, hour, and minute
    """

    import pandas as pd

    year, month, day, hour, minute, cpd = [], [], [], [], [], []                        # preallocate lists
    cpd_name = dataframe.columns[1]                                                     # get the cpd name

    # iterate through rows and append lists, seperating components of the datetime
    for index, value in dataframe.iterrows():
        year.append(value.datetime.year)
        month.append(value.datetime.month)
        day.append(value.datetime.day)
        hour.append(value.datetime.hour)
        minute.append(value.datetime.minute)
        cpd.append(value[cpd_name])

    # drop previous columns
    dataframe.drop(['datetime', cpd_name], axis=1, inplace=True)
    dataframe = dataframe.reset_index()
    dataframe.drop('index', axis=1, inplace=True)

    # append each list to the new dataframe in appropriate order
    for item in [year, month, day, hour, minute, cpd]:
        item = pd.Series(item)
        dataframe = dataframe.merge(item.to_frame(), left_index=True, right_index=True, how='inner')

    # rename columns
    dataframe.columns = ['year', 'month', 'day', 'hour', 'minute', cpd_name]

    return dataframe