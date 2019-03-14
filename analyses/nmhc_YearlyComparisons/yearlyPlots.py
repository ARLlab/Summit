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

## Put legend outside of plot
# https://stackoverflow.com/questions/4700614/how-to-put-the-legend-out-of-the-plot
from matplotlib.font_manager import FontProperties
fontP = FontProperties()
fontP.set_size('small')
legend([plot1], "title", prop=fontP)

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
    plt.xticks(np.arange(0,361,30)) # 365 has no nice divsors

    for j in numYears:
        # The x axes are the dates of the given year (j) converted to a day decimal values
        x = (((nmhcData.loc[(date >= j) & (date < (j+1)),'DecYear'].values)-j) * 365) + 1
        # The y axes is the mixing ratio of that given day, for the specific compound
        y = nmhcData.loc[(date >= j) & (date < (j+1)),compounds[int(i)]].values
        plt.plot(x,y,'.',alpha=0.5,label='%i'%j)
        plt.legend(bbox_to_anchor=(1.04,1), loc="upper left") # puts legend outside of graph

plt.show() # Displays all figures

## References
    # https://stackoverflow.com/questions/4700614/how-to-put-the-legend-out-of-the-plot/43439132#43439132
