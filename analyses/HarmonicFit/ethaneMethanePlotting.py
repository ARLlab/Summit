"""
Created on March 28th, 2019. This script works on preliminary plotting data from the CCGVU exported functions,
residuals, and other features in the Python environment.
"""

# Import libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
# TODO: Import Seaborn and look at those plotting features?

# Reading in file
root = r'C:\Users\ARL\Desktop\J_Summit\analyses\HarmonicFit'                            # Root File Location
data = pd.read_csv(root + r'\test1.txt', encoding='utf8', delim_whitespace=True)        # whitespace delimitter read
data = data.dropna(axis=1, how='any')                                                   # drop NaN columns

# Printing for Data Understanding
pd.set_option('display.max_columns', None)
print(data.columns)

# Exploratory Graphing
plt.figure(1)
plt.plot(data['date'], data['value'], '.', alpha=0.5, label='Original Data')
plt.plot(data['date'], data['function'], linewidth=2, alpha=0.8, label='Fitted Function')
plt.plot(data['date'], data['smooth'], linewidth=2, label='Smoothed Fitted Function')
plt.title('Ethane / Methane Ratio')
plt.xlabel('Decimal Year')
plt.ylabel('Mixing Ratio [ppb]')
plt.legend()

plt.figure(2)
plt.plot(data['date'], data['residuals'], '.', alpha=0.8, label='Normal Residuals')
plt.plot(data['date'], data['resid_smooth'], '.', alpha=0.2, label='Residuals from Smoothed Fit')
plt.plot(data['date'], data['smooth_resid'], linewidth=3, label='Smoothed Residual Line')
plt.title('Fitted Function Residuals')
plt.xlabel('Decimal Year')
plt.ylabel('Mixing Ratio [ppb]')
plt.legend()

plt.show()
