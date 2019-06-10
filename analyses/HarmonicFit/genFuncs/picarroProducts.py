"""
This function gathers data products for CO, CO2, and CH4 from ARL's Picarro Analyzer at Summit, Greenland. The data
folder 'Picarro' filled with data files will need to be updated anytime the graphs want to be updated. This data
folder is located on the ARL lab computer in the folder 'Summit Processing --> Data --> Picarro' Work to organize and
sort these files comes from Brendan Blanchard
"""

# import libraries
import pandas as pd
import os
import timeit
import numpy as np


def picarroProd():

    start_time = timeit.default_timer()
    # import data
    colnames = ['date', 'na', 'na1', 'na2', 'julian', 'na3', 'na4', 'status', 'na5', 'na6', 'na7', 'na8', 'na9', 'MPV',
                'na10', 'co', 'na11', 'co2', 'na12', 'ch4', 'na13']                 # temp column names

    badcols = []                                                                    # remove 'na' columns
    for colname in colnames:
        if 'na' in colname:
            badcols.append(colname)  # col names to delete

    direc = r'C:\Users\ARL\Desktop\PicarroSync\DataLog_User_Sync\2019'              # top level directory
    datafiles = []                                                                  # preallocate files list
    picarro = []
    for (dirpath, dirnames, filenames) in os.walk(direc):                           # walk thru dir and get all files
        datafiles.extend(filenames)                                                 # put them in the list
        for file in filenames:                                                      # loop through every data file
            path = dirpath + r'\{}'.format(file)                                    # full path
            data = pd.read_csv(path, delim_whitespace=True, error_bad_lines=False,
                               index_col=None, names=colnames, header=None)         # load data to pd DF
            data = data.drop(badcols, axis=1)                                       # drop 'na' columns
            picarro.append(data.values)                                             # append array vals to list

    picarro_comb = np.vstack(picarro)                                               # numpy vertical stack to concat
    dataFinal = pd.DataFrame(picarro_comb)                                          # turn back to DF

    dataFinal = dataFinal.dropna(axis=0, how='any')                                 # drop NaN values
    dataFinal.columns = [x for x in colnames if x not in badcols]                   # reset column names
    dataFinal = dataFinal[dataFinal['MPV'] == 963]                                  # only 963 MPV position

    print(timeit.default_timer() - start_time)


if __name__ == '__main__':
    picarroProd()

