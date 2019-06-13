
import pandas as pd

def noaaDateConv(dataframe):
    """
    This function takes a dataframe with datetime values and converts it into a format that the NOAA ccg tool can
    easily read

    :param dataframe: A dataframe that has to have a column labeled 'datetime' which contains dt.datetime formatted
    items
    :return: the same dataframe with the datetime column replaced by year, month, day, hour, and minute
    """

    year, month, day, hour, minute, cpd = [], [], [], [], [], []
    cpd_name = dataframe.columns[1]

    for index, value in dataframe.iterrows():
        year.append(value.datetime.year)
        month.append(value.datetime.month)
        day.append(value.datetime.day)
        hour.append(value.datetime.hour)
        minute.append(value.datetime.minute)
        cpd.append(value[cpd_name])

    dataframe.drop(['datetime', cpd_name], axis=1, inplace=True)
    for item in [year, month, day, hour, minute, cpd]:
        item = pd.Series(item)
        dataframe = dataframe.merge(item.to_frame(), left_index=True, right_index=True)

    dataframe.columns = ['year', 'month', 'day', 'hour', 'minute', cpd_name]

    return dataframe

