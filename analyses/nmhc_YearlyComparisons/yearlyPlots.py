"""
Created on Thursday, March 7th, 2019.

This script serves to develop plots of each specific NHMC Compound from 2008-2018,
Each compound is plotted over the course of a single year with each line representing
a separate year

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

## Import function
from fileInput import fileLoad
nmhcData = fileLoad(r"C:\Users\ARL\Desktop\Python Code\Data\NMHC.XLSX")

## Plotting Data
date = nmhcData.loc[:,'DecYear'] # Variable describing the decimal Year
numCompounds = np.linspace(0,11,num=12) # There are 12 compounds we want to plots
compounds = list(nmhcData.columns)[3:15] # List of the compound names
numYears = np.linspace(2008,2018,num=((2018 - 2008)+1)) # number of years total

for i in numCompounds:
    plt.figure(i) # Open a new fig for each compounds
    plt.xlabel('Day of Year',fontdict=None,labelpad=None) # x labels all same
    plot.ylabel('Mixing Ratio [Parts per Billion]',fontdict=None,labelpad=None) # y labels
    plt.title('Summit %s from 2008-2018' %compounds[int(i)],fontdict=None,pad=None)

    for j in numYears:
        # The x axes are the dates of the given year (j) converted to a day decimal values
        # The y axes is the mixing ratio of that given day, for the specific compound
        plt.plot((((nmhcData.loc[(date >= j) & (date < (j+1)),'DecYear'].values)-j) * 365) + 1\
                 ,nmhcData.loc[(date >= j) & (date < (j+1)),compounds[int(i)]].values,'.')

plt.show() # Displays all figures
