"""
Created on Tuesday, March 21st, 2019.

This script is nearly the same as ratios2.py but plots the ratio of
acetylene / methane.

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

nmhcDate = nmhcData.loc[:,'DecYear'] # Variable describing the decimal Year
ch4Date = methaneData.loc[:,'DecYear']
hrs3 = 3 * 60 * 60 # three hours in seconds
daytosec = 24 * 60 * 60 # convert days to seconds

from isleapyear import isleapyear
for i in np.linspace(2012,2018,num=((2018 - 2012)+1)):

    ## Define date variables for given year
    aceDate = nmhcData.loc[(nmhcDate >= i) & (nmhcDate < (i+1)),'DecYear'].values # Past 2012
    aceDate = (aceDate - i) * (365 + isleapyear(i)) * daytosec # Convert to seconds

    methaneDate= methaneData.loc[(ch4Date >= i) & (ch4Date < (i+1)),'DecYear'].values
    methaneDate = (methaneDate - i) * (365 + isleapyear(i))* daytosec

    ## Define other variables
    ace = nmhcData.loc[(nmhcDate >= i) & (nmhcDate < (i+1)),'acetylene'].values
    methane = methaneData.loc[(ch4Date >= i) & (ch4Date < (i+1)),'MR'].values

    aceMethane = np.zeros(np.size(ace)) # Preallocate ethaneMethane matrix

    ## Iterate over each value in ethane
    for j,value in np.ndenumerate(ace):
        high = aceDate[j] + hrs3 # Current Ethane timestep in seconds + 3 hours
        low = aceDate[j] - hrs3 # current ethane timestep in seconds - 3 hours
        # Get the average of all methane values between high and low
        methaneAverage = np.mean(methane[(methaneDate[:] <= high) & (methaneDate[:] >= low)])
        aceMethane[j] = value / methaneAverage # Puts ratios in matrix for plotting

    ## Plotting
    figure(1)
    plt.plot((aceDate / daytosec),aceMethane,'.',alpha=0.5,label='%i'%i)
    plt.xlabel('Day of Year',fontdict=None,labelpad=None) # Plot Xlabel
    plt.ylabel('Mixing Ratio [Parts per Billion]',fontdict=None,labelpad=None) # Plot Ylabel
    plt.title('Summit Acetylene / Methane from 2012-2018',fontdict=None,pad=None)
    plt.legend(bbox_to_anchor=(1.04,1),loc="upper left")
