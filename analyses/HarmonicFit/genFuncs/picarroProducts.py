"""
This function gathers data products for CO, CO2, and CH4 from ARL's Picarro Analyzer at Summit, Greenland. The data
folder 'Picarro' filled with data files will need to be updated anytime the graphs want to be updated. This data
folder is located on the ARL lab computer in the folder 'Summit Processing --> Data --> Picarro' Work to organize and
sort these files comes from Brendan Blanchard
"""

# import libraries
import pandas as pd
import os


def picarroProd():

    # import data
    colnames = ['']
    picarro = pd.DataFrame(columns=colnames)                                        # preallocate DF
    dir = r'C:\Users\ARL\Desktop\picarro'                                           # directory
    for file in os.listdir(dir):                                                    # for each file in dir
        data = pd.read_csv(file, delim_whitespace=True)                             # load data
        picarro = picarro.append(data)                                              # append to DF

    picarro = picarro.dropna(axis=0, how='any')                                     # drop NaN values
    coldrop = ['']                                                                  # columns to drop
    picarro = picarro.drop(coldrop, axis=1)                                         # drop columns


if __name__ == '__main__':
    picarroProd()
