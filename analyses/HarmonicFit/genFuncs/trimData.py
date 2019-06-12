"""
Created on Friday May 24th, 2019

This script imports data from current updated Methane and VOC spreadsheets, combines them with past data,
and optimizes them for usage in the NOAA Python CCG Harmonic Fitting Program, then outputs them to a text file.

This script returns the data specifically in the form optimized for the CCG Harmonic Fitting Program: a text file
with two columns of numbers, the first column is a decimal year value, and the second column is the corresponding
value, columns are separated just by white space

Currently, a few data cleaning tactics are employed. First, since there is no methane data past 2012, the VOC data
before 2012 is cut. We only look at ethane and acetylene in this circumstance, so other VOC columns are removed. Any
row with a NaN value is completely dropped. Since methane is sampled more frequently than the VOC's, methane values
from +/- three hours from any given VOC data point are averaged and then used to calculate the ratio of VOC / ch4 at
any given VOC datapoint timestep.
"""

# Import libraries
from fileInput import fileLoad
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Import Data Sets from Excel
nmhcData = fileLoad(r"C:\Users\ARL\Desktop\Python Code\Data\NMHC.xlsx")
methaneData = fileLoad(r"C:\Users\ARL\Desktop\Python Code\Data\Methane.xlsx")

# TODO: Combine with new data from currently updated spreadsheets once corrections are made?

# Cleaning Up Data
nmhcData = nmhcData[nmhcData['DecYear'] > 2012]                             # Only need years past 2012 in VOC Data
reqRows = ['DecYear', 'ethane', 'acetylene']                                # only need date, ethane, and acetylene
nmhcData = nmhcData[reqRows]                                                # just get required rows
methaneData = methaneData.dropna(axis=0, how='any')                         # Remove NaN values, entire row is removed
nmhcData = nmhcData.dropna(axis=0, how='any')

# Define preliminary variables
years = np.linspace(2012, 2018, num=((2018-2012)+1))                        # Array of Years
numYears = np.size(years)                                                   # Total number of years
nmhcDate = nmhcData['DecYear']                                              # Gather just the decimal year col
ch4Date = methaneData['DecYear']
hrs3 = 3 / 24 / 365                                                         # three hours in decimal years

ethaneMethane = []                                                          # preallocate arrays
aceMethane = []

for i in years:                                                             # MAIN LOOP - YEARS

    # Date Variables for given year
    currentDateVoc = nmhcDate.loc[(nmhcDate >= i) & (nmhcDate < (i + 1))]   # gathers current year
    currentDateCh4 = ch4Date.loc[(ch4Date >= i) & (ch4Date < (i+1))]

    # Compound Variables for given year
    ethane = pd.DataFrame(nmhcData.loc[(nmhcDate >= i) & (nmhcDate < (i + 1)), 'ethane'])          # Maintain dataframe
    ace = pd.DataFrame(nmhcData.loc[(nmhcDate >= i) & (nmhcDate < (i + 1)), 'acetylene'])
    methane = pd.DataFrame(methaneData.loc[(ch4Date >= i) & (ch4Date < (i + 1)), 'MR'])

    # Create Ratio Vectors, average methane values around VOC timepoints to address different dates

    for index, value in ethane.iterrows():                                  # LOOP: Ethane Values
        high = nmhcDate[index] + hrs3                                       # three hours above current VOC date
        low = nmhcDate[index] - hrs3                                        # three hours below
        indices = (currentDateCh4 <= high) & (currentDateCh4 >= low)        # get the indicies between these hours
        methaneAv = np.mean(methane[indices].values)                        # take the methane average of those index
        ethaneMethane.append(value.ethane / methaneAv)                      # append to the ratio array

    for index, value in ace.iterrows():                                     # LOOP: Acetylene Values
        high = nmhcDate[index] + hrs3                                       # Process same as above
        low = nmhcDate[index] - hrs3
        indices = (currentDateCh4 <= high) & (currentDateCh4 >= low)
        methaneAv = np.mean(methane[indices].values)
        aceMethane.append(value.acetylene / methaneAv)

# Create final text file with outputs

plt.plot(nmhcDate, ethaneMethane, '.')
plt.title('Test Plot Comparison to CCG')
plt.xlabel('Decimal Year')
plt.ylabel('Mixing Ratio [ppb]')
plt.show()

ethaneMethane = pd.DataFrame(ethaneMethane)                                 # Convert back to dataframe
ethaneMethane.columns = ['val']                                             # Give it a title
ethaneMethane = ethaneMethane.dropna(axis=0, how='any')                     # Remove any NaN Values
with open("ethaneMethane.txt", "w+") as f:                                  # Create a new text file
    for index, value in ethaneMethane.iterrows():                           # LOOP: Ratio Values
        f.write("%f " % nmhcDate.iloc[index])                               # Write the date with a whitespace
        f.write("%f\n" % value.val)                                         # Write the value and a newline

aceMethane = pd.DataFrame(aceMethane)                                       # Same as above
aceMethane.columns = ['val']
aceMethane = aceMethane.dropna(axis=0, how='any')
with open("aceMethane.txt", "w+") as f:
    for index, value in aceMethane.iterrows():
        f.write("%f " % nmhcDate.iloc[index])
        f.write("%f\n" % value.val)
