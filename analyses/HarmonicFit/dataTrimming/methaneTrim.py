"""
This script makes a few modifications to the Methane data from 2012-2018. Eventually it will also import new 2019
data from the spreadsheet. Created on May 29th, 2019
"""

# Import libraries
from fileInput import fileLoad
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

root = r'C:\Users\ARL\Desktop\J_Summit\analyses\HarmonicFit\textFiles'
methaneData = fileLoad(r"C:\Users\ARL\Desktop\Python Code\Data\Methane.xlsx")
methaneData = methaneData.dropna(axis=0, how='any')                         # Remove NaN values, entire row is removed

# Remove extreme outliers
flag1 = methaneData[methaneData['MR'] > 2100].index
flag2 = methaneData[methaneData['MR'] < 1730].index

methaneData = methaneData.drop(flag1)
methaneData = methaneData.drop(flag2)

print(methaneData.max())
print('-'*10)
print(methaneData.min())

with open(root + r"\methaneARL_nofit.txt", 'w+') as f:
    for index, value in methaneData.iterrows():
        f.write("%f " % value.DecYear)
        f.write("%f\n" % value.MR)

