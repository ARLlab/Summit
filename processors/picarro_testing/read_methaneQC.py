# Import Libraries
import pandas as pd
import matplotlib.pylab as plt


def check_cols_methane(name):
    """
    Mini Function passed to pd.read_excel(usecols=function)
    :return: Returns True for columns 22, 27, and 29
    """
    return True if name in ['SampleDay', 'SampleHour', 'Decimal Year',
                            'Peak Area 1', 'Peak Area 2', 'Run median', 'Daily Median'] else False


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
    run_median = data['Run median'].dropna(axis=0, how='any')  # Get values, remove NaN's
    daily_median = data['Daily Median'].dropna(axis=0, how='any')

    run_median_dates = data.loc[run_median.index, 'Decimal Year']  # Find cooresponding dates
    daily_median_dates = data.loc[daily_median.index, 'Decimal Year']

    run_median_dates = (run_median_dates - year) * 365  # convert to days
    daily_median_dates = (daily_median_dates - year) * 365

    # We can make this more specific with the isleapyear() function in Summit.analyses.Basic_Functions if you want to
    # use it

    # Plotting
    plt.figure(1)
    plt.plot(run_median_dates, run_median, 'r', label='Run Medians')
    plt.plot(daily_median_dates, daily_median, 'b', label='Daily Medians')
    plt.title('Methane Results in 2019')
    plt.xlabel('Day of Year')
    plt.ylabel('Mixing Ratio [ppb]')

    plt.show()

    return run_median, run_median_dates, daily_median, daily_median_dates


def brendan_test():
    from datetime import datetime
    import pandas as pd

    from summit_core import connect_to_db, merge_lists, search_for_attr_value
    from summit_core import methane_dir
    from summit_methane import SampleCorrection, Base, GcRun

    filename = r'Z:\Data\Summit_GC\Summit_GC_2019\CH4_results\SUM_CH4_insitu_2019.xlsx'
    #filename = r'/home/brendan/PycharmProjects/Summit/processors/summit_methane_processor/SUM_CH4_insitu_2019.xlsx'
    # Brendan's path

    year = filename.split('.')[-2][-4:]  # risky...

    engine, session = connect_to_db('sqlite:///summit_methane.sqlite', methane_dir)
    Base.metadata.create_all(engine)

    data = pd.read_excel(filename, usecols=check_cols_methane, sheet_name='data_2019')
    data = data[data['SampleDay'] != 0]  # filter out not-yet-present data

    counts = data.groupby(['SampleDay', 'SampleHour']).size()

    unique_runs = counts.index.tolist()
    # returns a list of tuples where each tuple is a unique (day,hour) combo

    gc_runs = session.query(GcRun)

    ct = 0
    for run in unique_runs:
        doy, hour = run[0], run[1]

        run_date = datetime.strptime(f'{year}{doy:03.0f}{hour:02.0f}', '%Y%j%H')

        matched_run = gc_runs.filter(GcRun.date == run_date).one_or_none()

        if not matched_run:
            print(f'No run matched for {run_date}.')
            continue  # for now...

        run_set = data[(data['SampleDay'] == doy) & (data['SampleHour'] == hour)][['Peak Area 1', 'Peak Area 2']]

        if run_set.shape != (5,2):
            print(f'Run for {run_date} did not have the correct dimensions and could not be processed.')

        peaks1 = run_set['Peak Area 1'].values.tolist()  # column of peaks, ordered 1,3,5,7,9
        peaks2 = run_set['Peak Area 2'].values.tolist()  # column of peaks, ordered 2,4,6,8,10

        ordered_peaks = merge_lists(peaks1, peaks2)  # returns peaks in order [0,1,2,3,4, ..., 9]

        run = None  # default to no run found yet

        corrections = []
        for num, pa in enumerate(ordered_peaks):
            """
            Finding samples in db:
            Use DOY, hour to find the run, then use run.id to get samples iteratively,
                if sample of x num does not exist, warn/ log an error (should have been created when reading log)
            
            """

            matched_sample = search_for_attr_value(matched_run.samples, 'sample_num', num)

            if not matched_sample:
                print(f'Matched sample not found for sample number {num} in GcRun for {matched_run.date}.')
                continue


            # sample.date = run.pa_line.date + dt.timedelta(minutes=peak.rt - 1)

            corrections.append(SampleCorrection(num, pa, matched_sample))

        # for sample in corrections:
        #     print(sample)
        #     print(sample.sample_num)
        #     print(sample.pa)

        for corr in corrections:
            session.merge(corr)

        ct += 1

        if ct > 50:
            continue

    # get number of data points for each day/hour combo

    # print(counts.where(counts != 5).dropna(how='all', axis='rows'))
    # warn these exist, they can't be safely interpreted

    session.commit()
    session.close()
    engine.dispose()

if __name__ == '__main__':
    # main()
    brendan_test()
