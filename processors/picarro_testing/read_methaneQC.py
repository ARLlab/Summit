# Import Libraries
import pandas as pd
import matplotlib.pylab as plt


def check_cols_methane(name):
    """
    Mini Function passed to pd.read_excel(usecols=function)
    :return: Returns True for columns 22, 27, and 29
    """
    return True if name == 'Decimal Year' or name == 'Run median' or name == 'Daily Median' else False


def main():
    """
    This function tests code to later be integrated into read_excel_corrections.py. The goal is to
    read QC methane run medians and daily medians from the sum_ch4 spreadsheet.

    :return:
    """

    # Import Required Functions

    # Read Excel Sheet
    filename = r'Z:\Data\Summit_GC\Summit_GC_2019\CH4_results\SUM_CH4_insitu_2019.xlsx'
    data = (pd.read_excel(filename, usecols=check_cols_methane, sheet_name='data_2019'))

    year = 2019

    # Format Data Appropriately
    run_median = data['Run median'].dropna(axis=0, how='any')               # Get values, remove NaN's
    daily_median = data['Daily Median'].dropna(axis=0, how='any')

    run_median_dates = data.loc[run_median.index, 'Decimal Year']           # Find cooresponding dates
    daily_median_dates = data.loc[daily_median.index, 'Decimal Year']

    run_median_dates = (run_median_dates - year) * 365                      # convert to days
    daily_median_dates = (daily_median_dates - year) * 365

    # Plotting
    plt.figure(1)
    plt.plot(run_median_dates, run_median, 'r', label='Run Medians')
    plt.plot(daily_median_dates, daily_median, 'b', label='Daily Medians')
    plt.title('Methane Results in 2019')
    plt.xlabel('Day of Year')
    plt.ylabel('Mixing Ratio [ppb]')

    plt.show()

    return run_median, run_median_dates, daily_median, daily_median_dates


if __name__ == '__main__':
    main()