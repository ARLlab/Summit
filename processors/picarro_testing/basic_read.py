import pandas as pd
import os

datadir = r'C:\Users\arl\Desktop\summit_master\processors\picarro_testing\data_test_set'

files = [file for file in os.scandir(datadir) if '.dat' in file.name]

MPV = 'MPVPosition'  # var for easy indexing

# for file in files:
# 	print(file.name)
# 	pd.read_csv(file, delim_whitespace=True)

all_data = pd.concat(pd.read_csv(file, delim_whitespace=True) for file in files)
all_data.set_index(pd.to_datetime(all_data['DATE'] + ' ' + all_data['TIME']), inplace=True)
print("All data", len(all_data))

question = all_data.loc[all_data[MPV] == 0].reset_index()
print("Data MPV == 0 ?", len(question))

ambients = all_data.loc[all_data[MPV] == 1].reset_index()
print("Ambients", len(ambients))

for comp in ['CH4', 'CO2']:
    print(f"The median of {comp} - {comp}_dry was {(ambients[comp] - ambients[comp + '_dry']).median():.04f}")

low_std = all_data.loc[all_data[MPV] == 2].reset_index()
print("Low standard", len(low_std))

high_std = all_data.loc[all_data[MPV] == 3].reset_index()
print("High standard", len(high_std))

mid_std = all_data.loc[all_data[MPV] == 4].reset_index()
print("Middle standard", len(mid_std))


def find_cal_indices(epoch_time):
    """
    Cal events are any time a standard is injected and being quantified by the system. Here, they're separated as though
    any calibration data that's more than 10s away from the previous cal data is a new event.

    :param epoch_time: array of epoch times for all of the supplied data
    :return: list of cal events indices, where each index is the beginning of a new cal event
    """
    epoch_diff = epoch_time.diff()
    indices = [i - 1 for i in epoch_diff.loc[epoch_diff > 10].index.values.tolist()]  # subtract one from all indices
    return indices


class CalEvent():
    cal_count = 0  # unique ID generator

    def __init__(self, DT, CH4, CO2, CO):
        self.id = CalEvent.cal_count
        CalEvent.cal_count += 1  # increment class var to get unique IDs
        self.datetime = DT
        self.CH4 = CH4 * 1000
        self.CO2 = CO2
        self.CO = CO * 1000
        self.plot_time = self.datetime.iat[-1]
        self.duration = self.datetime.iat[-1] - self.datetime.iat[0]
        self.cal_period = 20  # using a 20s back-window of calibration data

        self.CH4_mean = self.CH4[self.get_back_period(self.cal_period)].mean()
        self.CO2_mean = self.CO2[self.get_back_period(self.cal_period)].mean()
        self.CO_mean = self.CO[self.get_back_period(self.cal_period)].mean()

        self.CH4_med = self.CH4[self.get_back_period(self.cal_period)].median()
        self.CO2_med = self.CO2[self.get_back_period(self.cal_period)].median()
        self.CO_med = self.CO[self.get_back_period(self.cal_period)].median()

        self.CH4_std = self.CH4[self.get_back_period(self.cal_period)].std()
        self.CO2_std = self.CO2[self.get_back_period(self.cal_period)].std()
        self.CO_std = self.CO[self.get_back_period(self.cal_period)].std()

    def __str__(self):
        return f"<CalEvent {self.id} with {len(self.CH4)}, {len(self.CO2)}, {len(self.CO)}, CH4, CO2, and CO data points>"

    def __repr__(self):
        return f"<CalEvent {self.id} with {len(self.CH4)} CH4, CO2, and CO data points>"

    def get_datetime(self):
        return self.datetime

    def get_plot_time(self):
        return self.plot_time

    def get_id(self):
        return self.id

    def get_CH4(self):
        return self.CH4

    def get_CO2(self):
        return self.CO2

    def get_CO(self):
        return self.CO

    def get_mean(self, compound):
        assert compound in ('CH4', 'CO2', 'CO'), "Invalid compound given to get_mean()."
        return getattr(self, compound + '_mean', None)

    def get_median(self, compound):
        assert compound in ('CH4', 'CO2', 'CO'), "Invalid compound given to get_median()."
        return getattr(self, compound + '_med', None)

    def get_std(self, compound):
        assert compound in ('CH4', 'CO2', 'CO'), "Invalid compound given to get_std()."
        return getattr(self, compound + '_std', None)

    def get_back_period(self, seconds):
        """Retrieve boolean array such that self.CO2[bool_array] captures the last (given) seCOnds of data in the CalEvent"""
        start_t = self.datetime.iat[-1] - pd.to_timedelta(seconds, unit='s')
        return self.datetime >= start_t


standards = {'high_standard': high_std, 'mid_standard': mid_std, 'low_standard': low_std}
standard_eventdict = dict()

for name, standard in standards.items():
    print(f"############### The standard is now {name} #################")

    for comp in ['CH4', 'CO2']:
        print(f"The mean of {comp} - {comp}_dry was {(standard[comp] - standard[comp + '_dry']).mean():.04f}")

    standard_events = []
    prev_index = 0
    for ind in find_cal_indices(standard['EPOCH_TIME']):
        print(f'Creating CalEvent from {prev_index} to {ind} in {name}.')
        standard_events.append(CalEvent(standard.loc[prev_index:ind, 'index'],
                                        standard.loc[prev_index:ind, 'CH4_dry'],
                                        standard.loc[prev_index:ind, 'CO2_dry'],
                                        standard.loc[prev_index:ind, 'CO']))
        prev_index = ind + 1

    standard_events[:] = [ce for ce in standard_events if
                          ce.duration.seconds >= 110]  # remove any cal events shorter than 110s
    standard_eventdict[name] = standard_events

# for ce in standard_events:
# 	for secs in [20, 40, 60]:
# 		test = ce.get_back_period(secs)
# 		print(f"CalEvent {ce.get_id()} had a CH4 mean of {ce.get_CH4()[test].mean():.03f}, a median of {ce.get_CH4()[test].median():.03f}, and stdev of {ce.get_CH4()[test].std():.03f} for {secs} seCOnds back.")
# 	print('')
#
# for ce in standard_events:
# 	for secs in [20, 40, 60]:
# 		test = ce.get_back_period(secs)
# 		print(f"CalEvent {ce.get_id()} had a CO2 mean of {ce.get_CO2()[test].mean():.03f}, a median of {ce.get_CO2()[test].median():.03f}, and stdev of {ce.get_CO2()[test].std():.03f} for {secs} seCOnds back.")
# 	print('')
#
# for ce in standard_events:
# 	for secs in [20, 40, 60]:
# 		test = ce.get_back_period(secs)
# 		print(f"CalEvent {ce.get_id()} had a CO mean of {ce.get_CO()[test].mean():.03f}, a median of {ce.get_CO()[test].median():.03f}, and stdev of {ce.get_CO()[test].std():.03f} for {secs} seCOnds back.")
# 	print('')

"""
Toss calibration events under 1 min 50s, then plot mean, median and stdev(yy) over time for each standard
"""

# for name, standard_events in standard_eventdict.items():
# 	for ce in standard_events:
# 		print(f'{name} {ce.get_id()} Duration: {ce.duration.seconds} seconds')
#
# 		print(f"{ce.get_plot_time()} CH4: {ce.get_mean('CH4'):.03f} CO2: {ce.get_mean('CO2'):.03f} CO: {ce.get_mean('CO'):.03f}")

df_dict = dict()

for name, standard_events in standard_eventdict.items():
    df_dict[name] = pd.DataFrame({'id': [ce.get_id() for ce in standard_events],
                                  'date': [ce.get_plot_time() for ce in standard_events],
                                  'CH4_mean': [ce.get_mean('CH4') for ce in standard_events],
                                  'CO2_mean': [ce.get_mean('CO2') for ce in standard_events],
                                  'CO_mean': [ce.get_mean('CO') for ce in standard_events],
                                  'CH4_median': [ce.get_median('CH4') for ce in standard_events],
                                  'CO2_median': [ce.get_median('CO2') for ce in standard_events],
                                  'CO_median': [ce.get_median('CO') for ce in standard_events],
                                  'CH4_std': [ce.get_std('CH4') for ce in standard_events],
                                  'CO2_std': [ce.get_std('CO2') for ce in standard_events],
                                  'CO_std': [ce.get_std('CO') for ce in standard_events]})

high_limit_dict = {'CH4': (2035, 2055, 2050.6), 'CO2': (415, 435, 428.53),
                   'CO': (160, 190, 174.6)}  # values are (high limit, low limit, cert value)
mid_limit_dict = {'CH4': (1905, 1935, 1925.5), 'CO2': (390, 410, 408.65), 'CO': (100, 130, 117.4)}
low_limit_dict = {'CH4': (1820, 1850, 1838.5), 'CO2': (375, 395, 390.24), 'CO': (60, 90, 69.6)}


def plot_standard_compound(standard, compound, df):
    import matplotlib.pyplot as plt
    from matplotlib.dates import DateFormatter

    f1 = plt.figure()
    ax = f1.gca()

    compound_mean_col = compound + '_mean'
    compound_med_col = compound + '_median'
    compound_std_col = compound + '_std'

    ax.errorbar(df['date'], df[compound_mean_col], yerr=df[compound_std_col])
    ax.errorbar(df['date'], df[compound_med_col], yerr=df[compound_std_col])

    if name == 'low_standard':
        ax.plot([df['date'].iloc[0], df['date'].iloc[-1]],
                [low_limit_dict.get(compound)[2], low_limit_dict.get(compound)[2]])
        ax.set_ylim(bottom=low_limit_dict.get(compound)[0], top=low_limit_dict.get(compound)[1])

    elif name == 'mid_standard':
        ax.plot([df['date'].iloc[0], df['date'].iloc[-1]],
                [mid_limit_dict.get(compound)[2], mid_limit_dict.get(compound)[2]])
        ax.set_ylim(bottom=mid_limit_dict.get(compound)[0], top=mid_limit_dict.get(compound)[1])

    elif name == 'high_standard':
        ax.plot([df['date'].iloc[0], df['date'].iloc[-1]],
                [high_limit_dict.get(compound)[2], high_limit_dict.get(compound)[2]])
        ax.set_ylim(bottom=high_limit_dict.get(compound)[0], top=high_limit_dict.get(compound[1]))

    [i.set_linewidth(2) for i in ax.spines.values()]
    ax.tick_params(axis='x', labelrotation=30)
    ax.tick_params(axis='both', which='major', size=8, width=2, labelsize=15)

    date_form = DateFormatter("%Y-%m-%d")
    ax.xaxis.set_major_formatter(date_form)

    ax.set_title(f"{standard} {compound} Calibration Events")
    ax.legend(['Certified Value', compound_mean_col, compound_med_col])
    f1.set_size_inches(11.11, 7.406)
    f1.subplots_adjust(bottom=.20)

    f1.savefig(f"dry_{compound}_{standard}.png")


for name, df in df_dict.items():
    plot_standard_compound(name, 'CO2', df)
    plot_standard_compound(name, 'CO', df)
    plot_standard_compound(name, 'CH4', df)
