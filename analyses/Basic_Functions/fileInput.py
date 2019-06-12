"""
Created on Thursday, March 7th, 2019.

This script is a general function for importting excel spreadsheets
[ Changed on 3/12 from previous script requirements ]

This code was written in Spyder via Anaconda Distribution [Python 3.7]


"""
def fileLoad(filename):

    ## Import Library
    import pandas as pd

    ## Opening Data from XLSX files
    data = pd.read_excel(filename)
    return data
