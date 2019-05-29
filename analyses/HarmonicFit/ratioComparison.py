"""
Created on March 29th, 2019. This script compares the residuals from both the acetylene / methane ratio,
and the ethane / methane ratio. It utilizes matplotlib.pyplots subplotting features. Additionally it performs a
linear regression on both the normal residuals and the smoothed residuals to compare correlations.

This data (from test1.txt, aceDefault.txt) is from CCGVU with the DEFAULT settings. This is three polynomial terms,
and four harmonic terms, with no switching in longterm or shortterm settings on the fast fourier transform

Source(s):
https://realpython.com/linear-regression-in-python/
https://www.datacamp.com/community/tutorials/seaborn-python-tutorial#sm
"""

# Import Libraries
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression
import seaborn as sns

# Reading in file
root = r'C:\Users\ARL\Desktop\J_Summit\analyses\HarmonicFit'                              # Root File Location

ethane = pd.read_csv(root + r'\test1.txt', encoding='utf8', delim_whitespace=True)        # whitespace delimitter read
ethane = ethane.dropna(axis=1, how='any')

ace = pd.read_csv(root + r'\aceDefault.txt', encoding='utf8', delim_whitespace=True)
ace = ace.dropna(axis=1, how='any')

# Performing Linear Regressions
x1 = np.array(ethane['residuals']).reshape((-1, 1))                 # Create numpy array, make 2D for x
y1 = np.array(ace['residuals'])                                     # dependent variable should be 1D for lin reg

x2 = np.array(ethane['resid_smooth']).reshape((-1, 1))              # Same, but with smoothed resid
y2 = np.array(ace['resid_smooth'])

model1 = LinearRegression().fit(x1, y1)                             # create liner regression fit
rSquared1 = model1.score(x1, y1)                                    # assign coeff of determination
intercept1 = model1.intercept_                                      # assign intercept
slope1 = model1.coef_                                               # assign slope
print('Coefficient of Determination:', rSquared1)                   # print statements for verification
print('-'*10)
print('Intercept:', model1.intercept_)
print('-'*10)
print('Slope:', model1.coef_)

model2 = LinearRegression().fit(x2, y2)
rSquared2 = model2.score(x1, y1)
intercept2 = model2.intercept_
slope2 = model2.coef_

# Modifiy Dataframes and combine to optimize with seaborn
titles_old = ['residuals', 'resid_smooth']                                                  # old titles
ace_titles_new = ['resid_ace', 'smooth_ace']                                                # new titles
ethane_titles_new = ['resid_ethane', 'smooth_ethane']

# Drop all but the residual columns
ethaneDrop = ethane.drop(labels=(ethane.columns[np.logical_and(ethane.columns != titles_old[0],
                                                               ethane.columns != titles_old[1])]), axis=1)
aceDrop = ace.drop(labels=(ace.columns[np.logical_and(ace.columns != titles_old[0],
                                                      ace.columns != titles_old[1])]), axis=1)

ethaneDrop.columns = ethane_titles_new                                                      # apply new col names
aceDrop.columns = ace_titles_new
data = pd.concat([ethaneDrop, aceDrop], sort=False, axis=1)                                 # Combine DataSets

# Graphing with Seaborn
f, axes = plt.subplots(nrows=3)                                                             # setup figure
sns.set(style='ticks')                                                                      # import style options
sns.axes_style("darkgrid")
sns.despine()                                                                               # remove side axes
sns.set_context('paper')                                                                    # paper size
plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=0.5)  # adjust plot spacing

# Seaborn Regression plots with regression statistic tables
ax1 = sns.regplot(x='resid_ethane', y='resid_ace', data=data, ax=axes[0],
                  line_kws={'label': 'rSquared: {:1.5f}\n Slope: {:1.5f}\n'.format(
                      rSquared1, slope1[0])})
ax2 = sns.regplot(x='smooth_ethane', y='smooth_ace', data=data, ax=axes[1],
                  line_kws={'label': 'rSquared: {:1.5f}\n Slope: {:1.5f}\n'.format(
                      rSquared2, slope2[0])})
ax3 = sns.regplot(x='resid_ethane', y='resid_ace', data=data, ax=axes[2])
ax4 = sns.regplot(x='smooth_ethane', y='smooth_ace', data=data, ax=axes[2])

ax1.set_title('Standard Residuals', fontsize=12)                                            # subplot titling
ax2.set_title('Smoothed Residuals', fontsize=12)
ax3.set_title('Overlay of Both', fontsize=12)

for plot in [ax1, ax2, ax3, ax4]:                                                           # LOOP: Each subplot
    plot.set_ylabel('')                                                                     # Remove ylabel
    plot.set_xlabel('')                                                                     # Remove xlabel
    plot.set(xlim=(-.0004, .0007), ylim=(-.00012, .00020))                                  # Set Axis Limits

for plot in [ax1, ax2]:                                                                     # LOOP: Indiv Plots
    plot.get_lines()[0].set_color('red')                                                    # Change reg line color
    plot.legend()                                                                           # Create legend

f.text(0.5, 0.04, 'Ethane Residuals [ppb]', fontsize=14, ha='center')                       # Global xlabel
f.text(0.04, 0.5, 'Acetylene Residuals [ppb]', fontsize=14,
       va='center', rotation='vertical')                                                    # global ylabel

# sns jointplot of std residuals for greater detail
sns.set()
g = sns.jointplot(x='resid_ethane', y='resid_ace', data=data, kind="reg", xlim=(-.0004, .0007), ylim=(-.00012, .00020),
                  line_kws={'label': 'rSquared: {:1.5f}\n Slope: {:1.5f}\n'.format(rSquared1, slope1[0])})
g.set_axis_labels('Ethane Residual [ppb]', 'Acetylene Residual [ppb]', fontsize=12)
g.ax_joint.get_lines()[0].set_color('red')
plt.legend()

plt.show()
