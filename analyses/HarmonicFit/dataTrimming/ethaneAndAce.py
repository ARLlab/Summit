"""
Created on Thursday June 6th, 2019

This simple function creates optimized text files of ethane and acetylene to use with the NOAA Harmonic Curve. It is
not the ethane and acetylene ratios with methane, just the raw data.


"""
import numpy as np
from fileInput import fileLoad
from isleapyear import isleapyear


def ethaneAce():

    # Import Data Sets
    nmhcData = fileLoad(r"C:\Users\ARL\Desktop\Python Code\Data\NMHC.xlsx")

    # Cleaning Up Data
    nmhcData = nmhcData[nmhcData['DecYear'] > 2012]             # Only need years past 2012 in VOC Data
    reqRows = ['DecYear', 'ethane', 'acetylene']                # only need date, ethane, and acetylene
    nmhcData = nmhcData[reqRows]                                # just get required rows
    nmhcData = nmhcData.dropna(axis=0, how='any')

    with open('ethaneOriginal.txt', 'w+') as f:
        for index, value in nmhcData.iterrows():
            f.write('%f ' % value.DecYear)
            f.write('%f\n' % value.ethane)

    with open('aceOriginal.txt', 'w+') as f:
        for index, value in nmhcData.iterrows():
            f.write('%f ' % value.DecYear)
            f.write('%f\n' % value.acetylene)


if __name__ == '__main__':
    ethaneAce()
