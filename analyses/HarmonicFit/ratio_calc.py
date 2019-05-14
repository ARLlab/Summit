"""
Created on Thursday May 14nd, 2019

This script explores the ratio of ethane to methane, as well as acetylene to methane. It plots the ratio
along all of the years where the data is present, and then performs harmonic fitting on the plots.

Goals:
1) Import spreadsheet data
2) Create and output ratio numpy arrays
3) Turn into a function

"""
def ratioCalc():

    import numpy as np
    from fileInput import fileLoad
    from isleapyear import isleapyear

    # Import Data Sets
    nmhcData = fileLoad(r"C:\Users\ARL\Desktop\Python Code\Data\NMHC.xlsx")
    methaneData = fileLoad(r"C:\Users\ARL\Desktop\Python Code\Data\Methane.xlsx")

    numYears = np.linspace(2012, 2018, num=((2018-2012)+1))     # Total Number of Years in Dataset
    nmhcDateAll = nmhcData.loc[:, 'DecYear']                    # nmhc dates
    ch4Date = methaneData.loc[:, 'DecYear']                     # methane dates

    hrs3 = 3 * 60 * 60                                          # three hours in seconds

    for i in numYears:                                          # MAIN LOOP

        # Date Variables for given year
        nmhcDate = nmhcDateAll.loc[(nmhcDateAll >= i) & (nmhcDateAll < (i + 1))].values     # gathers current year
        nmhcDate = 1 + ((nmhcDate - i) * (365 + isleapyear(i)) * 24 * 60 * 60)              # convert to seconds

        methaneDate = ch4Date.loc[(ch4Date >= i) & (ch4Date < (i + 1))].values
        methaneDate = 1 + (methaneDate - i) * (365 + isleapyear(i)) * 24 * 60 * 60

        # Yearly compound values
        ethane = nmhcData.loc[(nmhcDateAll >= i) & (nmhcDateAll < (i + 1)), 'ethane'].values
        ace = nmhcData.loc[(nmhcDateAll >= i) & (nmhcDateAll < (i + 1)), 'acetylene'].values
        methane = methaneData.loc[(ch4Date >= i) & (ch4Date < (i + 1)), 'MR'].values

        # Preallocate Ratio Matrices
        ethaneMethane = np.zeros((np.size(numYears), np.size(ethane)))      # Columns are for each year
        aceMethane = np.zeros((np.size(numYears), np.size(ace)))            # Rows are for the actual ratio values

        # Create Ratio Vectors
        for j, value in np.ndenumerate(ethane):  # LOOP: Ethane values
            high = nmhcDate[j] + hrs3   # current Ethane timestep in seconds + 3 hours
            low = nmhcDate[j] - hrs3    # current ethane timestep in seconds - 3 hours

            # Get the average of all methane values between high and low
            methaneAverage = np.mean(methane[(methaneDate[:] <= high) & (methaneDate[:] >= low)])
            ethaneMethane[np.where(numYears == i), j] = value / methaneAverage       # Fills out matrix

        for k, value in np.ndenumerate(ace):    # LOOP: Acetylene Values
            high = nmhcDate[k] + hrs3           # Same process as above
            low = nmhcDate[k] - hrs3
            methaneAverage = np.mean(methane[(methaneDate[:] <= high) & (methaneDate[:] >= low)])
            aceMethane[np.where(numYears == i), k] = value / methaneAverage

    return ethaneMethane, aceMethane





