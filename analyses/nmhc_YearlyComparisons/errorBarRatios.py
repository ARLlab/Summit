"""
Created on Thursday May 2nd, 2019

This script explores the ratio of ethane to methane, as well as acetylene to methane.
It plots the background data, along with a connecting line of the daily mean across years
and then plots error bars of the standard deviation

This code was written in Spyder via Anaconda Distribution [Python 3.7]

"""
import time

# Import Libraries
import numpy as np
import matplotlib.pyplot as plt

# Change figure properties for presentation purposes
from matplotlib.pyplot import figure

# Import Data Sets
from fileInput import fileLoad
from isleapyear import isleapyear

# Import Data Sets
start = time.time() # timer for curiosity
nmhcData = fileLoad(r"C:\Users\ARL\Desktop\Python Code\Data\NMHC.XLSX")
methaneData = fileLoad(r"C:\Users\ARL\Desktop\Python Code\Data\Methane.XLSX")

numYears = np.linspace(2012, 2018, num=((2018 - 2012)+1))     # number of years total
nmhcDateAll = nmhcData.loc[:, 'DecYear']                     # date elements for nmhc
ch4Date = methaneData.loc[:, 'DecYear']                      # date elements for methane
hrs3 = 3 * 60 * 60                                          # three hours in seconds variable for ease

dailyMeanE = np.full((np.size(numYears), 366), np.nan)        # matrix preallocation
dailyMeanA = np.full((np.size(numYears), 366), np.nan)

for i in numYears:                                          # MAIN LOOP: Iterates over years

    # Date Variables for given year
    nmhcDate = nmhcDateAll.loc[(nmhcDateAll >= i) & (nmhcDateAll < (i+1))].values    # gathers current year
    nmhcDate = 1 + ((nmhcDate - i) * (365 + isleapyear(i)) * 24 * 60 * 60)           # convert to seconds

    methaneDate = ch4Date.loc[(ch4Date >= i) & (ch4Date < (i+1))].values
    methaneDate = 1 + (methaneDate - i) * (365 + isleapyear(i)) * 24 * 60 * 60

    # Yearly compound values
    ethane = nmhcData.loc[(nmhcDateAll >= i) & (nmhcDateAll < (i+1)), 'ethane'].values
    ace = nmhcData.loc[(nmhcDateAll >= i) & (nmhcDateAll < (i+1)), 'acetylene'].values
    methane = methaneData.loc[(ch4Date >= i) & (ch4Date < (i+1)), 'MR'].values

    # Preallocate Ratio Matrices
    ethaneMethane = np.zeros(np.size(ethane))
    aceMethane= np.zeros(np.size(ace))

    # Create Ratio Vectors
    for j,value in np.ndenumerate(ethane):        # LOOP: Ethane values
        high = nmhcDate[j] + hrs3                 # current Ethane timestep in seconds + 3 hours
        low = nmhcDate[j] - hrs3                  # current ethane timestep in seconds - 3 hours

        # Get the average of all methane values between high and low
        methaneAverage = np.mean(methane[(methaneDate[:] <= high) & (methaneDate[:] >= low)])
        ethaneMethane[j] = value / methaneAverage   # Puts ratios in matrix for plotting

    for k,value in np.ndenumerate(ace):             # LOOP: Acetylene Values
        high = nmhcDate[k] + hrs3                   # Same process as above
        low = nmhcDate[k] - hrs3
        methaneAverage = np.mean(methane[(methaneDate[:] <= high) & (methaneDate[:] >= low)])
        aceMethane[k] = value / methaneAverage

    # Plotting just the ratio data points
    figure(1)       # FIG 1: Ethane/Methane
    plt.plot((nmhcDate/60/60/24), ethaneMethane, '.', alpha=0.3, label='%i'%i)
    plt.xlabel('Day of Year', fontdict=None,labelpad=None)                       # Plot Xlabel
    plt.ylabel('Mixing Ratio [Parts per Billion]', fontdict=None, labelpad=None)  # Plot Ylabel
    plt.title('Summit Ethane / Methane from 2012-2018', fontdict=None, pad=None)
    plt.legend(bbox_to_anchor=(1.04, 1), loc="upper left")


    figure(2)       # FIG 2: Acetylene/Methane
    plt.plot((nmhcDate/60/60/24), aceMethane, '.', alpha=0.3, label='%i'%i)         # yearly labels
    plt.xlabel('Day of Year', fontdict=None, labelpad=None)                       # Plot Xlabel
    plt.ylabel('Mixing Ratio [Parts per Billion]', fontdict=None, labelpad=None)  # Plot Ylabel
    plt.title('Summit Acetylene / Methane from 2012-2018', fontdict=None, pad=None)
    plt.legend(bbox_to_anchor=(1.04, 1), loc="upper left")

    nmhcDate = np.around(nmhcDate / 60 / 60 / 24)                           # rounded day values
    for j in np.linspace(1, np.amax(nmhcDate), np.amax(nmhcDate)):
        index = np.where(np.isin(nmhcDate, int(j)))                          # indicies of current day
        meanE = np.mean(ethaneMethane[index])                               # mean of values on current day
        meanA = np.mean(aceMethane[index])

        # place in respective matrix
        dailyMeanE[int(i-2012), int(j-1)] = meanE
        dailyMeanA[int(i-2012), int(j-1)] = meanA

# Statistics across years
meanE = np.nanmean(dailyMeanE, axis=0)  # take the mean value of just the columns
meanA = np.nanmean(dailyMeanA, axis=0)
dateAxis = np.linspace(1, 366, 366)

stdE = np.nanstd(dailyMeanE, axis=0)    # take the standard deviation of columns
stdA = np.nanstd(dailyMeanA, axis=0)

figure(1)  # plotting mean
plt.plot(dateAxis , meanE ,'b',linewidth=2,label='Average Across Year')
plt.legend(bbox_to_anchor=(1.04 , 1) , loc="upper left")

figure(2)
plt.plot(dateAxis,meanA,'b',linewidth=2,label='Average Across Year')
plt.legend(bbox_to_anchor=(1.04,1),loc="upper left")

figure(1)  # plotting error bars
plt.errorbar(dateAxis,meanE,yerr=stdE,xerr=None, ecolor='b', errorevery=10, capsize=5)
figure(2)
plt.errorbar(dateAxis,meanA,yerr=stdA,xerr=None, ecolor='b', errorevery=10, capsize=5)

plt.show()

end = time.time()
print(end - start) # prints runtime
