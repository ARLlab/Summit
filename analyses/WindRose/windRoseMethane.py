"""
Created on June 7th, 2019. The met data used here is courtesy of NOAA ESRL GMD. ftp://ftp.cmdl.noaa.gov/met/sum/README,
a full citation can be found in the file 'metTrim.py'. This function creates a windrose plot of cardinal direction
with with methane data from the GCFID and from the Picarro Analyzer

This function uses raw methane data points and not the residuals from the NOAA GMD CCG Harmonic fitting tool. This is
because the short timespan of the picarro data results in a poor harmonic fit. Reanalysis should be done once a strong
seasonal cycle has been developed in the data.

"""

# import libraries and functions
import matplotlib.pyplot as plt
from WindRose.metTrim import metTrim, createDatetime
from dateConv import decToDatetime
from WindRose.windRoseConc import metCombo
import pandas as pd
import matplotlib.cm as cm
import windrose


def windRoseMethane():

    # ---- import data
    root = r'C:\Users\ARL\Desktop\J_Summit\analyses\Data'
    met = metTrim()
    arl = metCombo(root + r'\methane2019updated.txt')
    arl.dropna(axis=0, how='any', inplace=True)
    pic = pd.read_csv(root + r'\picarro_ch4.txt',
                      delim_whitespace=True, encoding='utf8', error_bad_lines=False, header=None)
    flask = pd.read_csv(root + r'\flask_ch4.txt', delim_whitespace=True, error_bad_lines=False, header=None)

    # ---- combining datasets (met and picarro)
    pic.columns = ['date', 'value']
    pic['datetime'] = decToDatetime(pic['date'].values)
    pic.drop('date', axis=1, inplace=True)
    met.drop(['steady'], axis=1, inplace=True)

    # merge the met data onto the concentration data by finding the nearest datetime within an hour
    pic.dropna(axis=0, how='any', inplace=True)
    picarro = pd.merge_asof(pic.sort_values('datetime'), met,
                            on='datetime',
                            direction='nearest',
                            tolerance=pd.Timedelta('1 hour'))
    picarro.dropna(axis=0, how='any', inplace=True)

    # ---- combining datasets (flask and met)
    met = metTrim()
    colnames = ['yr', 'mo', 'dy', 'hr', 'val']
    flask.columns = colnames
    flask['datetime'] = createDatetime(flask['yr'], flask['mo'],
                                       flask['dy'], flask['hr'])
    flask.drop(['yr', 'mo', 'dy', 'hr'], axis=1, inplace=True)
    earlyVals = (met['datetime'] <= flask['datetime'][0])
    met.drop(['steady'], axis=1, inplace=True)

    # merge the met data onto the concentration data by finding the nearest datetime within an hour
    flaskMet = pd.merge_asof(flask, met, on='datetime',
                             direction='nearest',
                             tolerance=pd.Timedelta('1 hour'))
    flaskMet.dropna(axis=0, how='any', inplace=True)

    # ---- plotting
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, subplot_kw=dict(projection='windrose'))
    fig.suptitle('Methane Conc. at Summit by Wind Direction', fontsize=16)
    plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.2, hspace=-0.3)

    # setup GC methane windrose
    ax1.bar(arl['dir'].values, arl['val'].values, normed=False, opening=0.9,
            edgecolor='black', nsector=24, bins=14, cmap=cm.viridis_r, blowto=False)
    ax1.set_title('GCFID Methane Conc. [ppb]\n')
    ax1.set_legend(loc=8, fancybox=True, shadow=True, bbox_to_anchor=(0.5, -1.05))

    # setup picarro methane windrose
    ax2.bar(picarro['dir'].values, picarro['value'].values, normed=False, opening=0.9,
            edgecolor='black', nsector=24, bins=14, cmap=cm.viridis_r, blowto=False)
    ax2.set_title('Picarro Methane Conc. [ppb]\n',)
    ax2.set_legend(loc=8, fancybox=True, shadow=True, bbox_to_anchor=(0.5, -1.05))

    # setup flask methane windrose
    ax3.bar(flaskMet['dir'].values, flaskMet['val'].values, normed=False, opening=0.9,
            edgecolor='black', nsector=24, bins=14, cmap=cm.viridis_r, blowto=False)
    ax3.set_title('Flask Methane Conc. [ppb]\n')
    ax3.set_legend(loc=8, fancybox=True, shadow=True, bbox_to_anchor=(0.5, -1.05))

    plt.show()


if __name__ == '__main__':
    windRoseMethane()

