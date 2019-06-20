"""
Created on June 4th, 2019. This script compares surface ozone data with hourly resolution from Summit (SUM) station
in Greenland with the ethane/methane and acetylene/methane ratios developed and analyzed in 'ratioComparison.py.

The ozone data used here is courtesy of NOAA ESRL GMD. See the citation below.

McClure-Begley, A., Petropavlovskikh, I., Oltmans, S., (2014) NOAA Global Monitoring Surface Ozone Network.
Summit, 2012-2018. National Oceanic and Atmospheric Administration, Earth Systems Research Laboratory
Global Monitoring Division. Boulder, CO. 5/30/2019. http://dx.doi.org/10.7289/V57P8WBF
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.linear_model import LinearRegression


def ozonePlot():
    # Importing Data
    root = r'C:\Users\ARL\Desktop\J_Summit\analyses\HarmonicFit\textfiles'                      # root source
    ozone = pd.read_csv(root + r'\ozone.txt', encoding='utf8', delim_whitespace=True,
                        header=None)                                                            # import as csv
    ethane = pd.read_csv(root + r'\test1.txt', encoding='utf8', delim_whitespace=True)
    ace = pd.read_csv(root + r'\aceDefault.txt', encoding='utf8', delim_whitespace=True)
    ozone.columns = ['date', 'value', 'function', 'resid', 'resid_smooth']
    print('Data Imported...')

    # Graphing with Seaborn -- Setup Subplots
    sns.set()
    f, ax = plt.subplots(ncols=2, nrows=2)                                                      # seaborn setup
    sns.despine(f)                                                                              # remove right/top axes
    flatui = ["#9b59b6", "#3498db", "#95a5a6", "#e74c3c", "#34495e", "#2ecc71"]                 # color palette
    sns.set_palette(flatui)
    plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=0.5)  # adjust plot spacing

    # ----
    # Background Ozone Data and Fitted Harmonic Functions
    ax1 = sns.scatterplot(x='date', y='value', data=ozone, ax=ax[0, 0],
                          alpha=0.7, s=10, legend='brief', label='Ozone Data')
    ax2 = sns.lineplot(x='date', y='function', data=ozone, ax=ax[0, 0],
                       linewidth=2, label='Fitted Curve')

    ax1.set_title('Ozone Surface MR from Summit', fontsize=12)
    ax1.set_xlabel('Decimal Year')
    ax1.set_ylabel('Mixing Ratio [ppb]')
    ax1.legend()
    print('Plot 1 Created')

    # ----
    # Ozone Residuals with smoothed residual curve
    ax3 = sns.scatterplot(x='date', y='resid', data=ozone, ax=ax[0, 1], alpha=0.7, s=10,
                          label='Ozone Residuals', legend='brief')
    ax4 = sns.lineplot(x='date', y='resid_smooth', data=ozone, ax=ax[0, 1], linewidth=2,
                       label='Smoothed Residual Curve')
    ax3.set_title('Ozone Residuals w/ Smoothed Curve')
    ax3.set_xlabel('Decimal Year')
    ax3.set_ylabel('Mixing Ratio [ppb]')
    ax3.legend()
    print('Plot 2 Created')

    # ----
    # Create new dataframes for residual comparison
    titles_old = ['date', 'value', 'residuals']
    ace_titles_new = ['date_ace', 'value_ace', 'resid_ace']
    eth_titles_new =['date_eth', 'value_eth', 'resid_eth']

    # drop unused columns for speed
    ethDrop = ethane.drop(labels=(ethane.columns[np.logical_and((np.logical_and(ethane.columns != titles_old[0],
                                                                                ethane.columns != titles_old[1])),
                                                                ethane.columns != titles_old[2])]), axis=1)
    aceDrop = ace.drop(labels=(ace.columns[np.logical_and((np.logical_and(ace.columns != titles_old[0],
                                                                          ace.columns != titles_old[1])),
                                                          ace.columns != titles_old[2])]), axis=1)

    ethDrop.columns = eth_titles_new                                                            # rename columns
    aceDrop.columns = ace_titles_new

    # Trim the data and assign equivalent date conditions
    earlyVals = ~(ozone['date'] < ethDrop['date_eth'][0])                                       # early ozone vals
    ozone = ozone[earlyVals]                                                                    # remove those vals
    ozone = ozone.reset_index()                                                                 # reset index
    ozone = ozone.drop('index', axis=1)                                                         # remove unnece column

    ozoneEthane = pd.concat([ozone, ethDrop], sort=False, axis=1)                               # combine datasets
    ozoneData = pd.concat([ozoneEthane, aceDrop], sort=False, axis=1)
    ozoneData['date_ace'] = ozoneData['date_eth']

    """
    This for loop is brutally slow, is there any way to fix this? Perhaps with a seperate function but that would take 
    the same length. I'm not sure if theres a built in function to reduce the size of an array by averging values into 
    the size of another array 
    """
    dataClean = []
    tolerence = 1 / 365                                                                         # ozone valus within day
    for index, value in ozoneData.iterrows():
        high = value.date_eth + tolerence                                                       # upper date lim
        low = value.date_eth - tolerence                                                        # lower date lim
        indices = (ozoneData['date'] <= high) & (ozoneData['date'] >= low)                      # indices between
        ozoneAv = np.nanmean(ozoneData['resid'][indices].values)                                # average of resids
        if ~np.isnan(value.resid_eth):
            dataClean.append([ozoneAv, value.resid_eth, value.resid_ace])                       # append clean mat

    ozoneFinal = pd.DataFrame(dataClean)                                                        # dataframe it
    ozoneFinal = ozoneFinal.dropna(axis=0, how='any')                                           # drop nans
    ozoneFinal.columns = ['OzoneResid', 'EthaneResid', 'AceResid']                              # rename columns

    # perform linear regression statistics
    x1 = np.array(ozoneFinal['EthaneResid']).reshape(-1, 1)
    y1 = np.array(ozoneFinal['OzoneResid'])
    model1 = LinearRegression().fit(x1, y1)                                                     # fit model
    rSquare1, intercept1, slope1 = model1.score(x1, y1), model1.intercept_, model1.coef_        # statistics

    x2 = np.array(ozoneFinal['AceResid']).reshape(-1, 1)
    y2 = np.array(ozoneFinal['OzoneResid'])
    model2 = LinearRegression().fit(x2, y2)                                                     # fit model
    rSquare2, intercept2, slope2 = model2.score(x2, y2), model2.intercept_, model2.coef_        # statistics

    # ----
    # Ethane Residuals v. Ozone Residuals
    ax5 = sns.regplot(x='EthaneResid', y='OzoneResid', data=ozoneFinal, ax=ax[1, 0],
                      line_kws={'label': 'rSquared: {:1.5f}\n Slope: {:1.5f}\n'.format(rSquare1, slope1[0])})
    ax5.set_title('Ethane/Ch4 Ratio Residuals v. Ozone Residuals')
    ax5.set_xlabel('Ethane/Ch4 Ratio Residuals [ppb]')
    ax5.set_ylabel('Ozone Residuals [ppb]')
    ax5.set(xlim=(-.0004, .0006))
    ax5.set(ylim=(-25, 25))
    ax5.legend()
    ax5.get_lines()[0].set_color('red')
    print('Plot 3 Completed')

    # ----
    # Acetylene Residuals v. Ozone Residuals
    ax6 = sns.regplot(x='AceResid', y='OzoneResid', data=ozoneFinal, ax=ax[1, 1],
                      line_kws={'label': 'rSquared: {:1.5f}\n Slope: {:1.5f}\n'.format(rSquare2, slope2[0])})
    ax6.set_title('Acetylene/Ch4 Ratio Residuals v. Ozone Residuals')
    ax6.set_xlabel('Acetylene/Ch4 Ratio Residuals [ppb]')
    ax6.set_ylabel('Ozone Residuals [ppb]')
    ax6.legend()
    ax6.get_lines()[0].set_color('red')
    print('Plot 4 Completed')

    plt.show()

    return ozone

if __name__ == '__main__':
    ozonePlot()




