
import pandas as pd

def noaaDateConv(dataframe):
    """
    This function takes a dataframe with datetime values and converts it into a format that the NOAA ccg tool can
    easily read

    :param dataframe: A dataframe that has to have a column labeled 'datetime' which contains dt.datetime formatted
    items
    :return: the same dataframe with the datetime column replaced by year, month, day, hour, and minute
    """

    year, month, day, hour, minute = [], [], [], [], []

    for index, value in dataframe.iterrows():
        year.append(value.datetime.year)
        month.append(value.datetime.month)
        day.append(value.datetime.day)
        hour.append(value.datetime.hour)
        minute.append(value.datetime.minute)

    dataframe.drop('datetime', axis=1, inplace=True)
    for item in [year, month, day, hour, minute]:
        item = pd.Series(item)
        dataframe = dataframe.merge(item.to_frame(), left_index=True, right_index=True)

    dataframe.columns = [dataframe.columns[0], 'year', 'month', 'day', 'hour', 'minute']

    return dataframe

