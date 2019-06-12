"""
Created on Wednesday June 12th, 2019

This function incorporates new data from the 2019 (and some from the 2018) NMHC ambient spreadsheets into a text file
that overwrites previous nmhc data.


"""
from fileInput import fileLoad
import pandas as pd
import datetime as dt
from decToDatetime import convToDatetime


def nmhc():

    # import original data set
    nmhcPrev = fileLoad(r"C:\Users\ARL\Desktop\Python Code\Data\NMHC.xlsx")
    nmhc2018 = fileLoad(r'Z:\Data\Summit_GC\Summit_GC_2018\NMHC_results\Ambient_2018_V2.xlsx')
    nmhc2019 = fileLoad(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\Ambient_2019.xlsx')

    # identify the mixing ratio rows
    allrows = list(range(0, len(nmhc2018.index)))
    rowstokeep = list(range(70, 94))
    rowstodrop = [x for x in allrows if x not in rowstokeep]

    # drop rows from nmhc2018 and nmhc2019
    nmhc2018 = nmhc2018.drop(rowstodrop, axis=0)
    nmhc2019 = nmhc2019.drop(rowstodrop, axis=0)

    # drop unnecesary columns and rows with nan, then cols with nan
    dropcols = ['Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3']
    nmhc2018, nmhc2019 = nmhc2018.drop(dropcols, axis=1), nmhc2019.drop(dropcols, axis=1)
    nmhc2018 = nmhc2018.dropna(axis=0, how='all', subset=[x for x in nmhc2018.columns if x not in ['Unnamed: 0']])
    nmhc2019 = nmhc2019.dropna(axis=0, how='all', subset=[x for x in nmhc2019.columns if x not in ['Unnamed: 0']])

    # transpose, reset columns, drop first row and last row
    nmhc2018, nmhc2019 = nmhc2018.T.reset_index(), nmhc2019.T.reset_index()
    nmhc2018.columns, nmhc2019.columns = list(nmhc2018.loc[0]), list(nmhc2019.loc[0])
    nmhc2018 = nmhc2018.drop([0, len(nmhc2018)-1], axis=0)
    nmhc2019 = nmhc2019.drop([0, len(nmhc2019)-1], axis=0)

    # create datetime column for each dataframe
    for yr in [nmhc2018, nmhc2019]:
        datetime = []
        sampledate = yr['Unnamed: 0'][1]
        yearstr = str(sampledate)[:4]
        yearint = int(yearstr)

        for x in yr[f'Decimal Day of Year {str(yearstr)[:4]}']:
            datetime.append(convToDatetime(yearint, x))

        yr['datetime'] = datetime

    # create datetime column for past data
    datetime = []
    for x in nmhcPrev['DecYear']:
        datetime.append(convToDatetime(0, x))
    nmhcPrev['datetime'] = datetime

    print('Debug Point')


if __name__ == '__main__':
    nmhc()