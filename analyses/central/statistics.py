"""
This central script contains functions related to statistic taking on datasets, such as finding and quanitifying
outliers, etc.
"""


def outliers(arr, zthresh, removal=False):
    """
    This function identifies outliers of a dataset for removal or plotting
    :param arr: a numpy array of values
    :param zthresh: limiting z score to define an outlier
    :param removal: default False. If true, outlier vals are removed from arr, and arr is returned
    :return: the subset of array defined as outliers
    """

    # importing
    import numpy as np
    from scipy import stats

    # z scores
    z = np.abs(stats.zscore(arr))
    outs = (z > zthresh)

    if removal:
        arr = arr[~outs]
        return arr
    else:
        subset = arr[outs]
        return subset
