"""
Created on Tuesday, March 19th, 2019.

This script plots the following ratios in a similar fashion to plotTest.py
1) Ethane / Methane
2) Acetylene / Methane

This code was written in Spyder via Anaconda Distribution [Python 3.7]

[METHOD 2 using df.apply]
"""

## Import Libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

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
    ethaneDate = nmhcData.loc[(nmhcDate >= i) & (nmhcDate < (i+1)),'DecYear'].values # Past 2012
    ethaneDate = (ethaneDate - i) * (365 + isleapyear(i)) * 24 * 60 * 60 # Convert to seconds

    methaneDate= methaneData.loc[(ch4Date >= i) & (ch4Date < (i+1)),'DecYear'].values
    methaneDate = (methaneDate - i) * (365 + isleapyear(i))* 24 * 60* 60

    ethane = nmhcData.loc[(nmhcDate >= i) & (nmhcDate < (i+1)),'ethane'].values # Gets ethane column, past 2012
    methane = methaneData.loc[(ch4Date >= i) & (ch4Date < (i+1)),'MR'].values

    # Applies methaneAverage function to the ethane block
    # ethaneMethane = ethane.applymap(methaneAverage)

    # Plot ethaneDate on x axis, ethane/methane on y axis
