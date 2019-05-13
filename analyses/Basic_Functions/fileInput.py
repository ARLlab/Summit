"""
Created on Thursday, March 7th, 2019.

This script is a general function for importting excel spreadsheets
[ Changed on 3/12 from previous script requirements ]

This code was written in Spyder via Anaconda Distribution [Python 3.7]

Overall Project Goals:
1) Times on the x axis should be written as day of the year instead of decimal
2) Each plot should have a proper legend
3) Remove outliers from the data sets
4) [Maybe?] Put each compound graph in one larger subplot for trend comparison
5) Modularize code to have an input / sort matrices file, and then a plotting file

"""
def fileLoad(filename):

    ## Import Library
    import pandas as pd

    ## Opening Data from XLSX files
    data = pd.read_excel(filename)
    return data
