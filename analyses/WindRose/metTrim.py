"""
Created on June 6th, 2019. This script imports surface met data from the Summit site and optimizes it for usage in
seaborn plotting in the form of a well labeled pandas dataframe

The met data used here is courtesy of NOAA ESRL GMD. See the citations below. ftp://ftp.cmdl.noaa.gov/met/sum/README

Mefford, T.K., M. Bieniulis, B. Halter, and J. Peterson,
Meteorological Measurements, in CMDL Summary Report 1994 - 1995,
No. 23, 1996, pg. 17.

Herbert, G., M. Bieniulis, T. Mefford, and K. Thaut, Acquisition
and Data Management Division, in CMDL Summary Report 1993,
No. 22, 1994, pg. 57

Herbert, G.A., J. Harris, M. Bieniulis, and J. McCutcheon,
Acquisition and Data Management, in CMDL Summary Report
1989, No. 18, 1990, Pg. 50.

Herbert, G.A., E.R. Green, G.L. Koenig, and K.W. Thaut,
Monitoring instrumentation for the continuous measurement
and quality assurance of meteorological observations, NOAA
Tech. Memo. ERL ARL-148, 44 pp, 1986.

Herbert, G.A., E.R. Green, J.M. Harris, G.L. Koenig, S.J.
Roughton, and K.W. Thaut, Control and Monitoring
Instrumentation for the Continuous Measurement of
Atmospheric CO2 and Meteorological Variables, J. of Atmos.
and Oceanic Tech., 3, 414-421, 1986.
"""

# Importing Libraries
import pandas as pd
import numpy as np

from dateConv import convDatetime, createDatetime


def metTrim():
    # ---- initial reading of data
    root = r'C:\Users\ARL\Desktop\MetData'
    ext = list(range(12, 20))                                                           # yearly extensions

    colnames = ['na', 'yr', 'mo', 'dy', 'hr', 'dir', 'spd', 'steady', 'na', 'na', 'na', 'na', 'na', 'na']
    met = pd.DataFrame(columns=colnames)                                                # preallocate df
    for yr in ext:
        # read in data
        data = pd.read_csv(root + r'\met_sum_insitu_1_obop_hour_20{}.txt'.format(yr), delim_whitespace=True,
                           header=None)
        data.columns = colnames                                                         # apply col names
        met = met.append(data)                                                          # append to list
    print('Data Imported')

    # ---- trimming data
    met = met.drop('na', axis=1)                                                        # drop na cols
    met = met.replace(-999.9, np.nan)                                                   # turn missing val to nan
    met = met.replace(-9, np.nan)
    met = met.replace(-999, np.nan)
    met = met.replace(-99.9, np.nan)
    met = met.dropna(axis=0, how='any')                                                 # remove rows with nan vals

    # ---- convert date to datetime
    metInt = met.applymap(int)                                                          # make sure values are ints
    dates = createDatetime(metInt['yr'].values,
                           metInt['mo'].values,
                           metInt['dy'].values,
                           metInt['hr'].values)

    met['datetime'] = dates                                                             # add it as a new column
    met = met.drop(['yr', 'mo', 'dy', 'hr'], axis=1)                                    # drop old date columns

    return met


if __name__ == '__main__':
    metTrim()



