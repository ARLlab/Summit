"""
Created on Thursday, March 7th, 2019.

This script serves to import data of NMHC compounds from 2008-2018.
The imported data is then parsed and converted for specific plotting

This code was written in Spyder via Anaconda Distribution [Python 3.7]

Overall Project Goals:
1) Times on the x axis should be written as day of the year instead of decimal
2) Each plot should have a proper legend
3) Remove outliers from the data sets
4) [Maybe?] Put each compound graph in one larger subplot for trend comparison
5) Modularize code to have an input / sort matrices file, and then a plotting file

"""

## Import Libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

## Opening Data from XLSX files
# Note: These excel files are the master data lists with some of my own modifications
file = r"C:\Users\ARL\Desktop\Python Code\Data\NMHC.XLSX"
nmhcData = pd.read_excel(file)

## Parsing Data into numpy arrays
nmhcDates = nmhcData.loc[:,'DecYear'].values
ethane = nmhcData.loc[:,'ethane'].values
ethene = nmhcData.loc[:,'ethene'].values
propane = nmhcData.loc[:,'propane'].values
propene = nmhcData.loc[:,'propene'].values
iButane = nmhcData.loc[:,'i-butane'].values
acetylene = nmhcData.loc[:,'acetylene'].values
nButane = nmhcData.loc[:,'n-butane'].values
iPentane = nmhcData.loc[:,'i-pentane'].values
nPentane = nmhcData.loc[:,'n-pentane'].values
hexane = nmhcData.loc[:,'hexane'].values
benzene = nmhcData.loc[:,'Benzene'].values
toluene = nmhcData.loc[:,'Toluene'].values

## Seperate Data into specific years

# nmhcDates is seperated into arrays for each year
    # Could iterate over each element and compare the number to bounds then index it
    # into its own vector but looping over this entire thing will be inefficient
dates2008 = nmhcDates[0:476:1]
dates2009 = nmhcDates[477:2182:1] # Currently this is hardcoded indexing
dates2010 = nmhcDates[2183:3067:1] # Although, this is bad coding practice
dates2012 = nmhcDates[3068:3709:1]
dates2013 = nmhcDates[3710:4616:1]
dates2014 = nmhcDates[4617:5315:1]
dates2015 = nmhcDates[5316:6215:1]
dates2016 = nmhcDates[6216:7248:1]
dates2017 = nmhcDates[7249:8111:1]
dates2018 = nmhcDates[8112:8794:1]

# Second, convert the decimal of the year into days
    # This converts to day of year, but how to differentiate multiple samples on one day?
    # Do I need to keep the hour time as well?
dates2008 = np.floor((dates2008 - 2008) * 365) + 1
dates2009 = np.floor((dates2009 - 2009) * 365) + 1
dates2010 = np.floor((dates2010 - 2010) * 365) + 1
dates2012 = np.floor((dates2012 - 2012) * 365) + 1
dates2013 = np.floor((dates2013 - 2013) * 365) + 1
dates2014 = np.floor((dates2014 - 2014) * 365) + 1
dates2015 = np.floor((dates2015 - 2015) * 365) + 1
dates2016 = np.floor((dates2016 - 2016) * 365) + 1
dates2017 = np.floor((dates2017 - 2017) * 365) + 1
dates2018 = np.floor((dates2018 - 2018) * 365) + 1
