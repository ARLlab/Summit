"""
fileLoading is a centralized script that contains a variety of functions useful for quickly importing datasets. All
datasets are somewhat different, but the functions here often provide a useful starting point and are shared in
analyses.

"""


def loadExcel(filename):
    """

    :param filename: full filepath
    :return: dataframe
    """

    import pandas as pd
    data = pd.read_excel(filename)
    return data


def readCsv(filename):
    """
    Reads a csv with typical options

    :param filename: full filepath
    :return: dataframe
    """

    import pandas as pd
    data = pd.read_csv(filename,
                       delim_whitespace=True,
                       encoding='utf-8',
                       header=None,
                       error_bad_lines=False,
                       warn_bad_lines=True,
                       engine='Python',
                       )
    return data
