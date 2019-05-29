"""
Created on March 29th, 2019. This script works on preliminary plotting data from the CCGVU exported functions,
residuals, and other features in the Python environment.

This data (from test1.txt) is from CCGVU with the DEFAULT settings. This is three polynomial terms, and four
harmonic terms, with no switching in longterm or shortterm settings on the fast fourier transform
"""

# Import Libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt

# Reading in File
root = r'C:\Users\ARL|Desktop\J_Summit\analyses\HarmonicFit'
data = pd.read_csv(root + r'\TextFiles\aceDefault.txt', encoding='utf8', delim_whitespace=True)
data = data.dropna(axis=1, how='any')

# Printing for Data Verification
pd.set_option('display.max_columns', None)
print(data.columns)
print('-'*10)
print(data.head)

# Exploratory Graphing
plt.figure(1)
plt.plot(data['date'], data['value'], '.', alpha=0.5, label='Original Data')
plt.plot(data['date'], data['function'], linewidth=2, alpha=0.8, label='Fitted Function')
plt.plot(data['date'], data['smooth'], linewidth=2, label='Smoothed Fitted Function')
plt.title('Acetylene / Methane Ratio')
plt.xlabel('Decimal Year')
plt.ylabel('Mixing Ratio [ppb]')
plt.legend()

plt.figure(2)
plt.plot(data['date'], data['residuals'], '.', alpha=0.8, label='Normal Residuals')
plt.plot(data['date'], data['resid_smooth'], '.', alpha=0.2, label='Residuals from Smoothed Fit')
plt.plot(data['date'], data['smooth_resid'], linewidth=3, label='Smoothed Residual Line')
plt.title('Fitted Function Residuals in Acetylene/Methane Ratio')
plt.xlabel('Decimal Year')
plt.ylabel('Mixing Ratio [ppb]')
plt.legend()

# Day of Year Plot of Residuals
doy = []                                                                        # Preallocate DOY List
for x in data['date']:
    start = x                                                                   # current date
    year = int(start)                                                           # Year of current date
    rem = start - year                                                          # reminder decimal (mo/day)
    base = dt.datetime(year, 1, 1)                                              # Base

    # Convert the current x value in the day by adding the timedelta of reminder in seconds
    result = base + dt.timedelta(seconds=(base.replace(year=base.year + 1) - base).total_seconds() * rem)

    julianDay = result.timetuple().tm_yday                                      # convert to Julian day
    doy.append(julianDay)                                                       # append the results

data.insert(1, 'DOY', doy)                                                      # insert into main datafrane

plt.figure(3)
plt.plot(data['DOY'], data['residuals'], '.', alpha=0.8, label='Normal Residuals')
plt.plot(data['DOY'], data['resid_smooth'], '.', alpha=0.2, label='Residuals from Smoothed Fit')
plt.title('Daily Residuals in Acetylene/Methane Ratio')
plt.xlabel('Decimal Year')
plt.ylabel('Mixing Ratio [ppb]')
plt.legend()

plt.show()
