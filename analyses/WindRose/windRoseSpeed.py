"""
Created on June 6th, 2019. The met data used here is courtesy of NOAA ESRL GMD. ftp://ftp.cmdl.noaa.gov/met/sum/README,
a full citation can be found in the file 'metTrim.py'. This function creates a windrose plot of the speed and
cardinal direction.

"""

# import libaries and functions
import matplotlib.pyplot as plt
from WindRose.metTrim import metTrim
import windrose as wr
import matplotlib.cm as cm


def metPlot():

    # import data
    met = metTrim()

    # wind rose plotting with just wind speed
    ax = wr.WindroseAxes.from_ax()
    ax.bar(met['dir'].values, met['spd'].values, normed=True, opening=0.9, edgecolor='black',
           nsector=24, bins=14, cmap=cm.viridis_r, blowto=False)
    ax.set_title('Wind Speed and Direction at Summit, Greenland')
    ax.set_xlabel('Wind Speed in Meters per Second')
    ax.set_legend()

    plt.show()

    return ax


if __name__ == '__main__':
    metPlot()

