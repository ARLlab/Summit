"""
Created on June 6th, 2019. The met data used here is courtesy of NOAA ESRL GMD. ftp://ftp.cmdl.noaa.gov/met/sum/README,
a full citation can be found in the file 'metTrim.py'. This function creates a windrose plot of the speed and
cardinal direction. Additionally it plots a version of the windrose plot with the concentration of both the
ethane/methane ratio and the acetylene/methane ratios.

"""

# import libaries and functions
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from metTrim import metTrim
from math import radians
import windrose as wr
import matplotlib.cm as cm
import numpy as np


def metPlot():

    # import data
    met = metTrim()

    # wind rose plotting with just wind speed
    ax = wr.WindroseAxes.from_ax()
    ax.bar(met['dir'].values, met['spd'].values, normed=True, opening=0.9, edgecolor='black',
           nsector=24, bins=14)
    ax.set_title('Wind Speed and Direction at Summit, Greenland')
    ax.set_xlabel('Wind Speed in Meters per Second')
    ax.set_legend()

    plt.show()


if __name__ == '__main__':
    metPlot()

