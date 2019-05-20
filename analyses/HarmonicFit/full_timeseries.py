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

# Plot the initial scatterplot data
figure(1)                                                                           # Ethane
plt.plot(datesFinal, ethaneMethane, '.', alpha=0.3, label='Background Data')
plt.title('Ethane Methane Ratio [2012-2018]')
plt.xlabel('Day of Year (Cumulative)')
plt.ylabel('Mixing Ratio [Parts per Billion]')

figure(2)                                                                           # Methane
plt.plot(datesFinal, aceMethane, '.', alpha=0.3, label='Background Data')
plt.title('Acetylene Methane from 2012 to 2018')
plt.xlabel('Day of Year')
plt.ylabel('Mixing Ratio [Parts per Billion]')

tau = 0.01                                                    # period for fourier transform?


# Fourier transformation func for give number of degrees
def fourier(x, *a):

    eq = a[0] * np.cos(np.pi / tau * x)                         # general eq
    for deg in range(1, len(a)):                                # depending on length of a, create that many eq's
        eq += a[deg] * np.cos((deg+1) * np.pi / tau * x)        # add them up

    return eq                                                   # return the final fourier eq


validData = ~(np.isnan(ethaneMethane))                          # remove NaN values from data

# Curve Fit using the Fourier equation, last piece is the number of equations given to fourier
numEqs = 4
params, covar = sp.optimize.curve_fit(fourier, datesFinal[validData], ethaneMethane[validData], p0=([1.0] * numEqs))

figure(1)
plt.plot(datesFinal, fourier(datesFinal, *params), '.', label='Fitted Wave')
plt.legend()

plt.show(1)

