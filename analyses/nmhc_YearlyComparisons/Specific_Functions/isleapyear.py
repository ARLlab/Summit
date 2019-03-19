"""
Created on Tuesday March 19th, 2019.

This script tells if an integer number is a leap year
Created for use with plotting nmhc Data from Summit GC

"""

def isleapyear(yr):

    ## Import Library
    import pandas as pd

    # Month and Day do not matter, just required. Converts to dataframe
    placeholder = pd.DataFrame({'year': [yr], 'month': [1], 'day':[1]})

    # Converts to the datetime format
    date = pd.to_datetime(placeholder)

    # Pandas function to tell if leap year
    leap = int(date.dt.is_leap_year)

    return leap
