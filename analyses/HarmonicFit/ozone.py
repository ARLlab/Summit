"""
Created on May 30th, 2019. This script compares surface ozone data with hourly resolution from Summit (SUM) station
in Greenland with the ethane/methane and acetylene/methane ratios developed and analyzed in 'ratioComparison.py.

The ozone data used here is courtesy of NOAA ESRL GMD. See the citation below.

McClure-Begley, A., Petropavlovskikh, I., Oltmans, S., (2014) NOAA Global Monitoring Surface Ozone Network.
 Summit, 2012-2018. National Oceanic and Atmospheric Administration, Earth Systems Research Laboratory
 Global Monitoring Division. Boulder, CO. 5/30/2019. http://dx.doi.org/10.7289/V57P8WBF

"""
# Importing Libraries
import pandas as pd
import numpy as np

# ---- initial reading of data
root = r'C:\Users\ARL\Desktop\OzoneData'

# Data needs to be read with python engine, comment out the headers
data = pd.read_csv(root + r'\sutptsoz12-18', encoding='utf8', delim_whitespace=True, error_bad_lines=False,
                   comment='#', engine='python')
# ---- data trimming
data = data.set_index('GMT')                                                    # make first column the index
badcol = ['MEAN', 'MAX']                                                        # columns we dont need
data = data.drop(badcol, axis=1)                                                # drop those columns
badrow = ['MEAN', 'GMT', 'DATE']                                                # rows we dont need
data = data.drop(badrow, axis=0)                                                # drop those rows

"""
each data point is an hour in a day, and leap years are accounted for. Since the first value is 01/01/2012 HR: 1, 
we can assume this is equivalent to 2012.0000 and just count each following data points value as another hour added 
to the current datetime index. Therfore we get the size of all the data points and make that many datetimes with each
one iterated by another hour. Technically the decimal derivitation of an hour will be off on the leap years but it 
should be inconsquential considering the sampling rate of the NMHC's which we will be averaging it up to.
"""
# create decimal dates for each data point
start = 2012.000                                                                # start date
oneHour = 1 / 365 / 24                                                          # one hour decimal
size = data.size                                                                # number of datevalues required
ozone = []                                                                      # preallocate dates

for i in range(size):                                                           # LOOP: as many items in data
    start += oneHour                                                            # iterate the start time
    ozone.append(start)                                                         # append it to dates

ozone = pd.DataFrame(ozone)                                                     # convert to dataframe
ozone.insert(1, 'col', pd.DataFrame(data.values.flatten()))                     # insert data values

finColumns = ['DecYear', 'MR']               # final column names
ozone.columns = finColumns                                                      # change col names

# additional data cleaning
ozone = ozone.replace(99.9, np.nan)                                             # replace 99 values with nan
ozone = ozone.replace(999.9, np.nan)                                            # replace 999 values with nan
ozone = ozone.dropna(axis=0, how='any')                                         # remove nan

# write to file
with open('ozoneInit.txt', "w+") as f:
    for index, value in ozone.iterrows():
        f.write("%f " % value.DecYear)
        f.write("%f\n" % value.MR)


