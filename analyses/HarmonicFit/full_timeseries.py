"""
Created on Thursday May 14nd, 2019

This script explores the ratio of ethane to methane, as well as acetylene to methane. It plots the ratio
along all of the years where the data is present, and then performs harmonic fitting on the plots.

Goals:
1) Import ratio data
2) Create timeseries of ratio data
3) Perform harmonic fit on the data
4) Identify regions of large residual


"""


import matplotlib.pyplot as plt
import numpy as np
import scipy as sp
from matplotlib.pyplot import figure
from ratio_calc import ratioCalc
from scipy import optimize

ethaneMethane, aceMethane, datesFinal = ratioCalc()     # call this function to get the ratio information

ethaneMethane = ethaneMethane.flatten('F')              # Flatten the arrays for ease of plotting
aceMethane = aceMethane.flatten('F')
datesFinal = datesFinal.flatten('F')

figure(2)                                               # Acetylene Methane Ratio Timeseries
plt.plot(datesFinal,aceMethane,'.',alpha=0.3)
plt.title('Acetylene Methane from 2012 to 2018')
plt.xlabel('Day of Year')
plt.ylabel('Mixing Ratio [Parts per Billion]')


# Function that SciPy will attempt to formulate
def test_func(x, a, b):
    return a * np.sin(b * x)


validData = ~(np.isnan(datesFinal) | np.isnan(ethaneMethane))
params, covar = sp.optimize.curve_fit(test_func, datesFinal[validData], ethaneMethane[validData])

figure(1)
plt.scatter(datesFinal, ethaneMethane, c='r', alpha=0.3, label='Background Data')
plt.plot(datesFinal, test_func(datesFinal, params[0], params[1]), label='Sin Fitted Wave')
plt.title('Ethane Methane Ratio [2012-2018]')
plt.xlabel('Day of Year (Cumulative)')
plt.ylabel('Mixing Ratio [Parts per Billion]')

plt.show()

