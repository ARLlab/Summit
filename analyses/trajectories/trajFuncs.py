
import pandas as pd
import os


def readTraj():
    """
    This function reads trajectory files specifically created from Pysplit for Hysplit 4.0
    :return: dataframe of trajectory features
    """
    root = r'C:\Users\ARL\Desktop\Jashan PySplit\pysplitprocessor-master\pysplitprocessor\trajectories'
    frame = []

    for filename in os.listdir(root):

        colnames = ['recep', 'v2', 'yr', 'mo', 'dy', 'hr', 'na', 'na1', 'backhrs', 'lat', 'long', 'alt', 'pres']
        data = pd.read_csv(root + '\\' + filename,                                  # read in the file
                           header=None,
                           delim_whitespace=True,                                   # whitespace
                           error_bad_lines=False,
                           names=colnames)                                          # skip bad lines

        data.dropna(axis=0, how='any', inplace=True)
        badcols = ['recep', 'v2', 'na', 'na1']
        data.drop(badcols, axis=1, inplace=True)                                    # drop bad columns
        data.reset_index(drop=True, inplace=True)                                   # reset index
        frame.append(data)                                                          # append to list

    traj = pd.concat(frame, axis=0, ignore_index=True)

    return traj


