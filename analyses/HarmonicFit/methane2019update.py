"""
June 17th, 2019. This python file combines past data with updated 2019 data points for further analysis.

"""

from fileLoading import loadExcel
from dateConv import noaaDateConv, decToDatetime
from scipy import stats

import pandas as pd
import datetime as dt
import calendar
import numpy as np


def methane():

    # import original dataset and new datasets
    methanePrev = loadExcel(r"C:\Users\ARL\Desktop\Python Code\Data\Methane.xlsx")
    methane2018 = loadExcel(r'C:\Users\ARL\Desktop\SUM_CH4_insitu_2018.xlsx')
    methane2019 = loadExcel(r'C:\Users\ARL\Desktop\Summit_GC_2019\CH4_results\SUM_CH4_insitu_2019.xlsx')

    # identify column names we want to keep
    goodcol = ['Decimal Year', 'Run median']                                                # good columns
    badcol = [x for x in methane2018.columns if x not in goodcol]                           # bad columns
    newnames = ['DecYear', 'MR']
    for sheet in [methane2018, methane2019]:
        sheet.drop(badcol, axis=1, inplace=True)                                            # drop bad columns
        sheet.dropna(how='any', axis=0, inplace=True)                                       # drop NaN rows
        sheet.columns = newnames                                                            # assign same col names

    methanePrev = methanePrev[methanePrev['DecYear'] < 2018]                                # remove some pre 2018 vals

    comb = [methanePrev, methane2018, methane2019]                                          # create combination frame
    methaneFinal = pd.concat(comb)                                                          # concat

    # trim extreme outliers
    values = methaneFinal['MR'].values
    z = np.abs(stats.zscore(values))
    thresh = 5
    methaneFinal = methaneFinal[~(z > thresh)]

    dates = decToDatetime(methaneFinal['DecYear'].values)                                        # conv to datetime
    methaneFinal['datetime'] = dates                                                        # add to dataframe

    noaaMethane = pd.DataFrame(columns=['datetime', 'MR'])
    noaaMethane['datetime'], noaaMethane['MR'] = dates, methaneFinal['MR'].values           # noaa version
    noaaMethane = noaaDateConv(noaaMethane)

    noaaMethane.to_csv('methane2019updated.txt', header=None, index=None, sep=' ', mode='w+')

    return methaneFinal


if __name__ == '__main__':
    methane()
