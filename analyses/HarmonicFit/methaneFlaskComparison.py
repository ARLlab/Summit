"""
Created on May 29th, 2019. This script performs multiple comparisons of methane data collected from the GC-FID at
Summit and methane data collected from the NOAA GMD Flask system at Summit. The data is fitted using the NOAA CCGVU
Fitting Tool. Plots are created using MatplotLib with the Seaborn extension.

This data (from methaneARL.txt, methaneFlask.txt) is from CCGVU with the DEFAULT settings. This is three polynomial
terms, and four harmonic terms, with no switching in longterm or shortterm settings on the fast fourier transform

Warning: Because of the size of the ARL GC Methane Data, this function can be extremely slow to plot. It takes 10 min
to 30 min to display the final plots. Speed Improvements can be made by turning the first and second subplots into
histogram distributions, or by removing a subset of the data for plotting.

"""

# Import Libraries
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.linear_model import LinearRegression

start = time.time()                                                                         # timer for curiosity
# Reading in File
root = r'C:\Users\ARL\Desktop\J_Summit\analyses\HarmonicFit\textFiles'
arl = pd.read_csv(root + r'\methaneARL.txt', encoding='utf8', delim_whitespace=True)
flask = pd.read_csv(root + r'\methaneFlask.txt', encoding='utf8', delim_whitespace=True)
print('Data Imported...')

# Graphing with Seaborn -- Setup Subplots
sns.set()                                                                                   # set seaborn
f, ax = plt.subplots(ncols=2, nrows=2)                                                      # set subplots
sns.despine(f)                                                                              # despine right + top
plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=0.5)  # adjust plot spacing

# ------------------
# Background Data Values & Fitted Harmonic Functions
ax1 = sns.scatterplot(x='date', y='value', data=arl, ax=ax[0, 0], alpha=0.7, s=10, legend='brief', label='GC Data')
ax2 = sns.scatterplot(x='date', y='value', data=flask, ax=ax[0, 0], alpha=0.7, s=10, legend='brief', label='Flask Data')

ax3 = sns.lineplot(x='date', y='function', data=arl, ax=ax[0, 0], linewidth=2, label='GC Fitted Curve')
ax4 = sns.lineplot(x='date', y='function', data=flask, ax=ax[0, 0], linewidth=2, label='Flask Fitted Curve')

ax1.set_title('GC v. Flask Methane Data')
ax1.set_xlabel('Decimal Year')                              # misc details
ax1.set_ylabel('Mixing Ratio [ppb]')
ax1.legend()
print('Plot 1 Created...')

# ------------------
# Residuals from Fitted Functions & Fitted Residual Curves
ax5 = sns.scatterplot(x='date', y='residuals', data=arl, ax=ax[0, 1], alpha=0.7, s=10, legend='brief',
                      label='GC Residuals')
ax6 = sns.scatterplot(x='date', y='residuals', data=flask, ax=ax[0, 1], alpha=0.7, s=10, legend='brief',
                      label='Flask Residuals')
ax7 = sns.lineplot(x='date', y='smooth_resid', data=arl, ax=ax[0, 1], linewidth=2, label='GC Resid Curve')
ax8 = sns.lineplot(x='date', y='smooth_resid', data=flask, ax=ax[0, 1], linewidth=2, label='Flask Resid Curve')

ax5.set_title('GC v. Flask Methane Residuals from Harmonic Fits')
ax5.set_xlabel('Decimal Year')                              # misc details
ax5.set_ylabel('Mixing Ratio [ppb]')
ax5.legend()
print('Plot 2 Created...')

# ------------------
# Modifiy Dataframes and combine to optimize with seaborn plotting
titles_old = ['date', 'value', 'residuals']                                             # old titles
gc_titles_new = ['date_gc', 'value_GC', 'Resid_GC']                                     # new titles
flask_titles_new = ['date_flask', 'value_flask', 'Resid_Flask']

# Drop Unused Columns for Speed
arlDrop = arl.drop(labels=(arl.columns[np.logical_and((np.logical_and(arl.columns != titles_old[0],
                                                                      arl.columns != titles_old[1])),
                                       arl.columns != titles_old[2])]), axis=1)
flaskDrop = flask.drop(labels=(flask.columns[np.logical_and((np.logical_and(flask.columns != titles_old[0],
                                                                            flask.columns != titles_old[1])),
                                             flask.columns != titles_old[2])]), axis=1)

arlDrop.columns = gc_titles_new                                                         # Renaming Columns
flaskDrop.columns = flask_titles_new
data = pd.concat([arlDrop, flaskDrop], sort=False, axis=1)                              # Combine DataSets

# Cleaning Data so each Flask data point has a matching GC, remove extraneous leftovers
dataClean = []                                                                          # preallocate mat
earlyVals = data[data['date_gc'] > data['date_flask']]                                  # remove flask vals w/ no GC
data = data.append(earlyVals).drop_duplicates(keep=False)                               # drop dupes removes them

tolerence = 2 / 365                                                                     # GC values within 2 days
for index, value in data.iterrows():                                                    # Loop through items in data
    high = value.date_flask + tolerence                                                 # Upper date limit
    low = value.date_flask - tolerence                                                  # Lower date limit
    indices = (data['date_gc'] <= high) & (data['date_gc'] >= low)                      # Indicies
    gc_av = np.nanmean(data['Resid_GC'][indices].values)                                   # GC avg at those indices
    if ~np.isnan(value.Resid_Flask):                                                    # append new mat unless res null
        dataClean.append([gc_av, value.Resid_Flask, value.value_GC, value.value_flask])

dataClean = pd.DataFrame(dataClean)                                                     # transform to DF
dataClean = dataClean.dropna(axis=0, how='any')                                         # Drop Leftover NaN
dataClean.columns = ['Resid_GC', 'Resid_Flask', 'value_GC', 'value_flask']              # rename columns

# Performing Linear Regression Statistics
x = np.array(dataClean['Resid_GC']).reshape((-1, 1))
y = np.array(dataClean['Resid_Flask'])
model = LinearRegression().fit(x, y)                                                    # fit model
rSquare, intercept, slope = model.score(x, y), model.intercept_, model.coef_            # statistics
print('Data Cleaned and Regression Line Fitted...')

# ------------------
# Residual Linear Regression Plot
ax9 = sns.regplot(x='Resid_GC', y='Resid_Flask', data=dataClean, ax=ax[1, 0],
                    line_kws={'label': 'rSquared: {:1.5f}\n Slope: {:1.5f}\n'.format(rSquare, slope[0])})
ax9.set_title('Comparison of GC and Flask Residuals to Harmonic Fit')
ax9.set_xlabel('GC Residuals [ppb]')
ax9.set_ylabel('Flask Residuals [ppb]')
ax9.legend()
ax9.get_lines()[0].set_color('red')                                                     # Change reg line color
print('Plot 3 Completed...')

# ------------------
# Residual Between Flask and GC
ax10 = sns.residplot(x='value_GC', y='value_flask', data=dataClean, ax=ax[1, 1], lowess=True)
ax10.set_title('Residual between GC and Flask Methane Data')
ax10.set_xlabel('GC Methane Data [ppb]')
ax10.set_ylabel('Residual from Flask Methane')
ax10.set(xlim=(1750, 2000))                                                        # Set Axis Limits
print('Full Plotting Completed...')

plt.show()

end = time.time()
print('Total Time [Seconds]:', end - start)

