
import pandas as pd

def noaaDateConv(dataframe):
    """
    This function takes a dataframe with datetime values and converts it into a format that the NOAA ccg tool can
    easily read

    :param dataframe: A dataframe that has to have a column labeled 'datetime' which contains dt.datetime formatted
    items
    :return: the same dataframe with the datetime column replaced by year, month, day, hour, and minute
    """

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

