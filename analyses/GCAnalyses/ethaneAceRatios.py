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
from scipy import stats
import matplotlib


matplotlib.rc('xtick', labelsize=22)
matplotlib.rc('ytick', labelsize=22)


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
        ax1.set_title(sheet.name + ' / Methane Ratio', size=26)
        ax1.set_xlabel('Datetime', fontsize=22)
        ax1.set_ylabel('Ratio Value', fontsize=18)
        ax1.set(xlim=((min(sheet['datetime']) - dt.timedelta(days=10)),
                      (max(sheet['datetime']) + dt.timedelta(days=10))))
        ax1.set(ylim=(min(sheet['value']) - np.mean(sheet['value']/3),
                      max(sheet['value']) + np.mean(sheet['value']/3)))
        ax2.get_lines()[0].set_color('purple')
        ax1.legend(prop={'size': 14})

        ax3 = sns.scatterplot(x='datetime', y='resid', data=sheet, alpha=0.7, label='Residuals', ax=ax[1])
        ax4 = sns.lineplot(x='datetime', y='residsmooth', data=sheet, linewidth=2, label='Smoothed Residual Fit',
                           ax=ax[1])
        ax4.get_lines()[0].set_color('purple')
        ax3.set_title('Residuals in ' + sheet.name, size=26)
        ax3.set_xlabel('Datetime', fontsize=22)
        ax3.set_ylabel('Residual / Value', fontsize=18)
        ax3.set(xlim=((min(sheet['datetime']) - dt.timedelta(days=10)),
                      (max(sheet['datetime']) + dt.timedelta(days=10))))
        ax3.set(ylim=(np.mean(sheet['resid']) - np.std(sheet['resid']) * 8,
                      np.mean(sheet['resid']) + np.std(sheet['resid']) * 8))
        ax3.legend(prop={'size': 14})

        # day of year plot residuals
        doy = []
        for x in sheet['datetime']:
            tt = x.timetuple()
            doy.append(tt.tm_yday)
        sheet['DOY'] = doy

        ax5 = sns.scatterplot(x='DOY', y='resid', data=sheet, alpha=0.7, label='Residuals', ax=ax[2])
        ax5.set_title('Residuals by Julian Day', size=26)
        ax5.set_xlabel('Day of Year', fontsize=22)
        ax5.set_ylabel('Residual / Value', fontsize=18)
        ax5.set(xlim=((min(sheet['DOY'])),
                      (max(sheet['DOY']))))
        ax5.set(ylim=(np.mean(sheet['resid']) - np.std(sheet['resid']) * 8,
                      np.mean(sheet['resid']) + np.std(sheet['resid']) * 8))
        ax5.legend(prop={'size': 14})

        direc = r'C:\Users\ARL\Desktop\Summit\analyses\Figures' + '\\' + sheet.name + 'Ratio.png'
        f.savefig(direc, format='png')

        for ax in [ax1, ax2, ax3, ax4, ax5]:
            ax.tick_params(labelsize=18)

    matplotlib.rc("legend", fontsize=26)

    # plotting seperate heatmap
    sns.set(style="white", font_scale=1.5)
    sns.despine()
    combo = pd.merge_asof(ethane, ace, on='datetime', direction='nearest')
    combo.dropna(axis=0, inplace=True, how='any')
    combo.drop(combo.index[5586:5776], axis=0, inplace=True)

    x = np.array(combo['resid_x']).reshape((-1, 1))
    y = np.array(combo['resid_y'])

    model = LinearRegression().fit(x, y)  # create liner regression fit
    rSquared = model.score(x, y)  # assign coeff of determination
    slope = model.coef_  # assign slope

    g = sns.jointplot(combo['resid_x'], combo['resid_y'], kind='reg', color='#e65c00',
                      line_kws={'label': 'rSquared: {:1.5f}\n Slope: {:1.5f}\n'.format(rSquared, slope[0])})
    g.set_axis_labels('Ethane/Methane Ratio', 'Acetylene/Methane Ration', fontsize=20)
    plt.tick_params(axis='both', labelsize=18)
    g.fig.suptitle('Correlation between Ethane and Acetylene Ratio Residuals', fontsize=28)
    g.ax_joint.get_lines()[0].set_color('blue')
    plt.legend()
    plt.show()


if __name__ == '__main__':
    ratioPlot()
