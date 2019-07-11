"""
This function does some exploration in identifiying if fires coorespond to high outliers in the acetylene/methane
ratio.

"""
from fileLoading import readCsv
from dateConv import decToDatetime
from fireFuncs import fireCombo

import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import pandas as pd
import numpy as np


def fireTrack():

    # import alternate data
    root = r'C:\Users\ARL\Desktop\Summit\analyses\Data'
    ace = readCsv(root + '\\' + r'aceRatioNoaa.txt')

    # import fire data
    virrs = False
    root = r'C:\Users\ARL\Desktop\FireData'
    if virrs:
        fire = pd.read_csv(root + r'\fire_archive_V1_60132.csv')
    else:
        fire = pd.read_csv(root + r'\fire_archive_M6_60131.csv')

    # data triming, reassign headers, add datetime column
    header = ['decyear', 'value', 'function', 'resid', 'residsmooth']
    ace.columns = header

    ace = ace[ace['value'] >= 0.00000001]

    ace['datetime'] = decToDatetime(ace['decyear'].values)
    ace['normResid'] = ace['resid'].values / ace['value'].values

    # combine fire and other dataset to produce master dataframe for analysis
    master = fireCombo(fire, ace, VIRRS=virrs)

    # identify average z score
    avg_vals = np.average(master['value_z'].values)
    avg_norms = np.average(master['normed_z'].values)

    print(f'The average z score in values is {avg_vals}')
    print(f'The average z score in normalized residuals is {avg_norms}')

    mybounds = {'x': (-73.2, -9.4),
                'y': (57.8, 84.3)}

    # scatterplot mapping
    img = mpimg.imread(root + r'\greenland.PNG')

    if virrs:
        master.plot(kind='scatter', x='longitude', y='latitude',
                    c='bright_ti4', cmap=plt.get_cmap('magma_r'),
                    colorbar=True, figsize=(10, 7))
    else:
        master.plot(kind='scatter', x='longitude', y='latitude',
                    c='brightness', cmap=plt.get_cmap('magma_r'),
                    colorbar=True, figsize=(10, 7))

    plt.imshow(img, extent=[mybounds['x'][0], mybounds['x'][1],
                                mybounds['y'][0], mybounds['y'][1]], alpha=0.5)

    plt.xlabel('Longitude', fontsize=14)
    plt.ylabel('Latitude', fontsize=14)
    if virrs:
        plt.title('NASA VIIRS Fire Count Overlay on Greenland')
    else:
        plt.title('NASA MODIS Fire Count Overlay on Greenland')
    plt.legend()
    plt.show()


if __name__ == '__main__':
    fireTrack()

