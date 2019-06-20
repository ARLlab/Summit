# importing
from pandas.plotting import register_matplotlib_converters
from dateConv import decToDatetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import datetime as dt
import seaborn as sns
import calendar

def ch4plot():

    header = ['yr', 'value', 'function', 'resid', 'residLine']                                  # dataframe headers
    root = r'C:\Users\ARL\Desktop\J_Summit\analyses\Data'
    data = pd.read_csv(root + '\\' + 'methane.txt', delim_whitespace=True, header=None,
                       encoding='utf8', error_bad_lines=False)
    data.columns = header

    register_matplotlib_converters()

    # convert the dec year col to datetime
    dates = dateConv(data['yr'])
    data['datetime'] = dates
    data.drop('yr', axis=1, inplace=True)

    # y bounds
    values = data['value']
    mean = np.mean(values)
    lowV = min(values) - (mean / 100)  # arbitrary vals look ok
    highV = max(values) + (mean / 100)

    mean = np.mean(data['resid'].values)
    lowR = min(data['resid']) - (mean / 3)
    highR = max(data['resid']) + (mean / 3)

    # x bounds
    low = min(data['datetime']) - dt.timedelta(days=30)
    high = max(data['datetime']) + dt.timedelta(days=30)

    # sample data for speed
    # data = data.sample(5000)

    # plotting
    sns.set()  # setup
    f, ax = plt.subplots(nrows=2, figsize=(12, 8))  # 2 column subplot
    sns.despine(f)
    plt.subplots_adjust(left=None, bottom=None, right=None,
                        top=None, wspace=0.3, hspace=0.5)

    # background data values with fitted harmonic functions
    ax1 = sns.scatterplot(x='datetime', y='value', data=data, ax=ax[0],
                          alpha=0.7, s=10, legend='brief', label='GC Data')
    ax2 = sns.lineplot(x='datetime', y='function', data=data, ax=ax[0], linewidth=2,
                       label='Fitted Curve')

    ax1.set_title('GC Methane Data with Fitted Function')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Mixing Ratio [ppb]')
    ax1.set(xlim=(low, high))
    ax1.set(ylim=(lowV, highV))
    ax1.get_lines()[0].set_color('#00b386')
    ax1.legend()

    # residual data
    ax3 = sns.scatterplot(x='datetime', y='resid', data=data, ax=ax[1],
                          alpha=1, s=10, legend='brief', label='Residuals from Fit')
    ax4 = sns.lineplot(x='datetime', y='residLine', data=data, ax=ax[1], linewidth=2,
                       label='Fitted Residual Curve')
    ax3.set_title('GC Residuals from Fitted Function')
    ax3.set_xlabel('Date')
    ax3.set_ylabel('Mixing Ratio [ppb]')
    ax4.get_lines()[0].set_color('#00b386')
    ax3.legend()
    ax3.set(xlim=(low, high))
    ax3.set(ylim=(lowR, highR))

    # save the plots
    direc = r'C:\Users\ARL\Desktop\J_Summit\analyses\Figures' + '\\' + 'methane.png'
    f.savefig(direc, format='png')


if __name__ == '__main__':
    ch4plot()
