"""
Created on Tuesday, March 19th, 2019.

This script plots the following ratio in a similar fashion to plotTest.py
1) Ethane / Methane

This code was written in Spyder via Anaconda Distribution [Python 3.7]

"""
## Import Libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from matplotlib.pyplot import figure
figure(num=None, figsize=(8, 6), dpi=160, facecolor='w', edgecolor='k')

## Import Data Sets
from fileInput import fileLoad
nmhcData = fileLoad(r"C:\Users\ARL\Desktop\Python Code\Data\NMHC.XLSX")
methaneData = fileLoad(r"C:\Users\ARL\Desktop\Python Code\Data\Methane.XLSX")

numYears = np.linspace(2012,2018,num=((2018 - 2012)+1)) # number of years total
nmhcDate = nmhcData.loc[:,'DecYear'] # Variable describing the decimal Year
ch4Date = methaneData.loc[:,'DecYear']
hrs3 = 3 * 60 * 60 # three hours in seconds

from isleapyear import isleapyear
for i in numYears:
    ## Define date variables for given year
    ethaneDate = nmhcData.loc[(nmhcDate >= i) & (nmhcDate < (i+1)),'DecYear'].values # Past 2012
    ethaneDate = (ethaneDate - i) * (365 + isleapyear(i)) * 24 * 60 * 60 # Convert to seconds

    methaneDate= methaneData.loc[(ch4Date >= i) & (ch4Date < (i+1)),'DecYear'].values
    methaneDate = (methaneDate - i) * (365 + isleapyear(i))* 24 * 60* 60

    ethane = nmhcData.loc[(nmhcDate >= i) & (nmhcDate < (i+1)),'ethane'].values # Gets ethane column, past 2012
    methane = methaneData.loc[(ch4Date >= i) & (ch4Date < (i+1)),'MR'].values

    ethaneMethane = np.zeros(np.size(ethane)) # Preallocate ethaneMethane matrix

    ## Iterate over each value in ethane
    for j,value in np.ndenumerate(ethane):
        high = ethaneDate[j] + hrs3 # Current Ethane timestep in seconds + 3 hours
        low = ethaneDate[j] - hrs3 # current ethane timestep in seconds - 3 hours
        # Get the average of all methane values between high and low
        methaneAverage = np.mean(methane[(methaneDate[:] <= high) & (methaneDate[:] >= low)])
        ethaneMethane[j] = value / methaneAverage # Puts ratios in matrix for plotting

    ## Plotting
    plt.plot((ethaneDate/60/60/24),ethaneMethane,'.',alpha=0.5,label='%i'%i)
    plt.xlabel('Day of Year',fontdict=None,labelpad=None) # Plot Xlabel
    plt.ylabel('Mixing Ratio',fontdict=None,labelpad=None) # Plot Ylabel
    plt.title('Summit Ethane / Methane from 2012-2018',fontdict=None,pad=None)
    plt.legend(bbox_to_anchor=(1.04,1),loc="upper left")
