"""
Created on May 30th, 2019. This script compares surface ozone data with hourly resolution from Summit (SUM) station
in Greenland with the ethane/methane and acetylene/methane ratios developed and analyzed in 'ratioComparison.py.

The ozone data used here is courtesy of NOAA GMD. See the citation below.

McClure-Begley, A., Petropavlovskikh, I., Oltmans, S., (2014) NOAA Global Monitoring Surface Ozone Network.
 Summit, 2012-2018. National Oceanic and Atmospheric Administration, Earth Systems Research Laboratory
 Global Monitoring Division. Boulder, CO. 5/30/2019. http://dx.doi.org/10.7289/V57P8WBF

"""
# Importing Libraries
import pandas as pd
import datetime as dt
from toYearFraction import toYearFraction

# Read the Data
root = r'C:\Users\ARL\Desktop\OzoneData'                                        # folder root
years = [2012, 2013, 2014]                            # data years
fileExt = [12, 13, 14]                                          # data file extensions
numYears = len(years)                                                           # total number of years
ozone = []                                                                      # preallocate data mat

for yr, ext in zip(years, fileExt):                                             # loop over all the years
    filename = root + r'\%s\sutccl%s' % (yr, ext)                               # full file location
    data = pd.read_csv(filename, encoding='utf8', delim_whitespace=True)        # read to DF
    ozone.append(data)                                                          # append to list

ozone = pd.DataFrame(ozone)                                                     # convert to DF
print(ozone.head(10))
print('-'*10)
print(ozone.isnull().all())
print('-'*10)
print(ozone.columns)

date = dt.datetime(ozone['year'], ozone['month'], ozone['day'], ozone['hour'])
decYear = toYearFraction(date)

