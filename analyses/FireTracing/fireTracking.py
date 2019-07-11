"""
This function does some exploration in identifiying if fires coorespond to high outliers in the acetylene/methane
ratio.

"""
from fileLoading import readCsv
from dateConv import decToDatetime
from fireFuncs import fireCombo

import pandas as pd
import numpy as np

# import fire data
root = r'C:\Users\ARL\Desktop\FireData'
fire = pd.read_csv(root + r'\fire_archive_V1_58066.csv')

# import alternate data
root = r'C:\Users\ARL\Desktop\Summit\analyses\Data'
ace = readCsv(root + '\\' + r'aceRatioNoaa.txt')

# data triming, reassign headers, add datetime column
header = ['decyear', 'value', 'function', 'resid', 'residsmooth']
ace.columns = header

ace = ace[ace['value'] >= 0.00000001]

ace['datetime'] = decToDatetime(ace['decyear'].values)
ace['normResid'] = ace['resid'].values / ace['value'].values

# combine fire and other dataset to produce master dataframe for analysis
master = fireCombo(fire, ace)

# identify average z score


print('debug point')
