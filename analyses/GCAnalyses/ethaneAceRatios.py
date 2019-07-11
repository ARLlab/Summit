"""
This script plots the updated ethane/methane and acetylene/methane ratios with new 2019 data and better coding
practices. Created 06/18/2019

"""

# import libraries
from pandas.plotting import register_matplotlib_converters
from sklearn.linear_model import LinearRegression
from dateConv import decToDatetime
from fileLoading import readCsv

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import datetime as dt
import numpy as np


def ratioPlot():
    register_matplotlib_converters()

    # import data
    root = r'C:\Users\ARL\Desktop\Summit\analyses\Data'
    ethane = readCsv(root + r'\ethaneRatioNoaa.txt')
    ace = readCsv(root + r'\aceRatioNoaa.txt')

    # data triming, reassign headers, add datetime column
    header = ['decyear', 'value', 'function', 'resid', 'residsmooth']

    for sheet in [ethane, ace]:
        sheet.columns = header

    ethane = ethane[ethane['value'] >= 0.0000001]
    ace = ace[ace['value'] >= 0.00000001]
    ethane.name = 'Ethane'
    ace.name = 'Acetylene'

    for sheet in [ethane, ace]:

        sheet['datetime'] = decToDatetime(sheet['decyear'].values)

        normResid = sheet['resid'].values / sheet['value'].values
        normSmooth = sheet['residsmooth'].values / sheet['value'].values

        sheet.drop(['resid', 'residsmooth'], axis=1, inplace=True)
        sheet['resid'] = normResid
        sheet['residsmooth'] = normSmooth

        if sheet.name == 'Ethane':
            ethane = sheet
        else:
            ace = sheet

        # plotting
        sns.set()
        f, ax = plt.subplots(nrows=3, figsize=(12, 8))
        sns.despine(f)
        plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=0.8)
        ax1 = sns.scatterplot(x='datetime', y='value', data=sheet, alpha=0.7, label='Original Data', ax=ax[0])
        ax2 = sns.lineplot(x='datetime', y='function', data=sheet, linewidth=2, label='Fitted Function', ax=ax[0])
        ax1.set_title(sheet.name + ' / Methane Ratio')
        ax1.set_xlabel('Datetime')
        ax1.set_ylabel('Mixing Ratio [ppb]')
        ax1.set(xlim=((min(sheet['datetime']) - dt.timedelta(days=10)),
                      (max(sheet['datetime']) + dt.timedelta(days=10))))
        ax1.set(ylim=(min(sheet['value']) - np.mean(sheet['value']/3),
                      max(sheet['value']) + np.mean(sheet['value']/3)))
        ax2.get_lines()[0].set_color('purple')
        ax1.legend()

        ax3 = sns.scatterplot(x='datetime', y='resid', data=sheet, alpha=0.7, label='Normalized Residuals', ax=ax[1])
        ax4 = sns.lineplot(x='datetime', y='residsmooth', data=sheet, linewidth=2, label='Smoothed Residual Fit',
                           ax=ax[1])
        ax4.get_lines()[0].set_color('purple')
        ax3.set_title('Normalized Residuals in ' + sheet.name)
        ax3.set_xlabel('Datetime')
        ax3.set_ylabel('Mixing Ratio [ppb]')
        ax3.set(xlim=((min(sheet['datetime']) - dt.timedelta(days=10)),
                      (max(sheet['datetime']) + dt.timedelta(days=10))))
        ax3.set(ylim=(np.mean(sheet['resid']) - np.std(sheet['resid']) * 8,
                      np.mean(sheet['resid']) + np.std(sheet['resid']) * 8))
        ax3.legend()

        # day of year plot residuals
        doy = []
        for x in sheet['datetime']:
            tt = x.timetuple()
            doy.append(tt.tm_yday)
        sheet['DOY'] = doy

        ax5 = sns.scatterplot(x='DOY', y='resid', data=sheet, alpha=0.7, label='Normalized Residuals', ax=ax[2])
        ax5.set_title('Normalized Residuals by Julian Day')
        ax5.set_xlabel('Day of Year')
        ax5.set_ylabel('Mixing Ratio [ppb]')
        ax5.set(xlim=((min(sheet['DOY'])),
                      (max(sheet['DOY']))))
        ax5.set(ylim=(np.mean(sheet['resid']) - np.std(sheet['resid']) * 8,
                      np.mean(sheet['resid']) + np.std(sheet['resid']) * 8))
        ax5.legend()

        direc = r'C:\Users\ARL\Desktop\J_Summit\analyses\Figures' + '\\' + sheet.name + 'Ratio.png'
        f.savefig(direc, format='png')

    # plotting seperate heatmap
    sns.set(style="white")
    sns.despine()
    combo = pd.merge_asof(ethane, ace, on='datetime', direction='nearest')
    combo = combo[combo['resid_y'] > -5]

    x = np.array(combo['resid_x']).reshape((-1, 1))
    y = np.array(combo['resid_y'])

    model = LinearRegression().fit(x, y)  # create liner regression fit
    rSquared = model.score(x, y)  # assign coeff of determination
    slope = model.coef_  # assign slope

    g = sns.jointplot(combo['resid_x'], combo['resid_y'], kind='reg', color='#e65c00',
                      line_kws={'label': 'rSquared: {:1.5f}\n Slope: {:1.5f}\n'.format(rSquared, slope[0])})
    g.set_axis_labels('Ethane MR [ppb]', 'Acetylene MR [ppb]', fontsize=12)
    g.fig.suptitle('Correlation between Ethane and Acetylene Normalized Residuals')
    g.ax_joint.get_lines()[0].set_color('blue')
    plt.legend()
    plt.show()


if __name__ == '__main__':
    ratioPlot()
