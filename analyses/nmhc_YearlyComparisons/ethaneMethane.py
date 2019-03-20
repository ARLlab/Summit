"""
Created on Tuesday, March 14th, 2019.

This script plots the following ratios in a similar fashion to plotTest.py
1) Ethane / Methane
2) Acetylene / Methane

This code was written in Spyder via Anaconda Distribution [Python 3.7]

"""

## Import Libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

## Import Data Sets
from fileInput import fileLoad
nmhcData = fileLoad(r"C:\Users\ARL\Desktop\Python Code\Data\NMHC.XLSX")
methaneData = fileLoad(r"C:\Users\ARL\Desktop\Python Code\Data\Methane.XLSX")

## Ethane / Methane
dateCH4 = methaneData.loc[:,'DecYear']
dateNMHC = nmhcData.loc[:,'DecYear']
numYears = np.linspace(2012,2018,num=((2018-2012)+1))

# Method 1
"""
Method isn't terrible but it takes long to run and I believe it is currently
misrepresenting the edge cases (since I just assume 1 value there even though
the times wouldn't match up exactly)

legend currently not working
"""
for i in numYears:
    x = (((nmhcData.loc[(dateNMHC >= i) & (dateNMHC < (i+1)),'DecYear'].values)-i) * 365) + 1
    ethane = nmhcData.loc[(dateNMHC >= i) & (dateNMHC < (i+1)),'ethane'].values

    plt.figure(1)
    plt.title('Summit Ethane/Methane Ratio from 2012-2018',fontdict=None,pad=None)
    plt.xlabel('Day of Year',fontdict=None,labelpad=None) # x labels all same
    plt.ylabel('Mixing Ratio [Parts per Billion]',fontdict=None,labelpad=None) # y labels

    for j in np.arange(0,ethane.size):
        # Averages seven CH4 values for each ethane value for use in the ratio
        methane = (methaneData.loc[(j+7)-3,'MR'] + methaneData.loc[(j+7),'MR'] + \
                   methaneData.loc[(j+7)-1,'MR'] + methaneData.loc[(j+7),'MR'] + \
                   methaneData.loc[(j+7)+1,'MR'] + methaneData.loc[(j+7)+2,'MR'] + \
                   methaneData.loc[(j+7)+3,'MR']) / 7

        # Upper and lower bounds of j statement stops indexing of false indicies
        if (j < 3) or (j > (ethane.size - 4)):
            # For now, take the singular methane value until better solution is found
            methane = methaneData.loc[j+7,'MR']

        # The ratio is plotted against the day of year
        plt.plot(x[j],(ethane[j] / methane),'.')

# plt.legend(bbox_to_anchor=(1.04,1), loc="upper left") # puts legend outside of graph
plt.show()

"""
# Method 2 - Some form of Broadcasting ethane onto methane?
for i in numYears:
    x = (((methaneData.loc[(dateCH4 >= i) & (dateCH4 < (i+1)),'DecYear'].values)-i) * 365) + 1
    ethane = nmhcData.loc[(dateNMHC >= i) & (dateNMHC < (i+1)),'ethane'].values
    methane = methaneData.loc[(dateCH4 >= i) & (dateCH4 < (i+1)),'MR'].values
    ratio = np.divide(ethane,methane)
"""
