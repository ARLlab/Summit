import pandas as pd
from fileLoading import loadExcel, readCsv
import os
from dateConv import visitToDatetime, createDatetime
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
from pandas.plotting import register_matplotlib_converters
import matplotlib.dates as mdates # For formatting date

register_matplotlib_converters()

# import the data
root = r'C:\Users\ARL\Desktop\Jashan\Summit\analyses\Data'
datapath = os.path.join(root, 'TAWO_visit_log.xlsx')
visits = loadExcel(datapath)
concpath = os.path.join(root, 'ethane.txt')
ethane = readCsv(concpath)

# data cleaning
dates = visits['Date'].values
dates = dates[1:]
badcols = ['Initials', 'Unnamed: 5', 'Date']
visits.drop(badcols, axis=1, inplace=True)
visits.drop([0], axis=0, inplace=True)
visits.dropna(axis=0, how='all', inplace=True)
visits.reset_index(drop=True, inplace=True)

ethane.columns = ['yr', 'mo', 'dy', 'hr', 'na', 'val']


# create proper datetimes
visits['start'], visits['end'] = visitToDatetime(dates,
                                                 visits['Arrival time (Z)'].values,
                                                 visits['Departure time (Z)'].values)
ethane['datetime'] = createDatetime(ethane['yr'].values, ethane['mo'].values,
                                    ethane['dy'].values, ethane['hr'].values)

# ethane cleaning
ethane.drop(['yr', 'mo', 'dy', 'hr', 'na'], axis=1, inplace=True)
ethane.dropna(how='any', inplace=True)
ethane.reset_index(drop=True, inplace=True)

# remove leftover columns
badcols = ['Arrival time (Z)', 'Departure time (Z)']
visits.drop(badcols, axis=1, inplace=True)

# calculate middle date time
visits['datetime'] = visits['start'] + (visits['end'] - visits['start'])/2


combo = pd.merge_asof(visits.sort_values('datetime'),
                      ethane.sort_values('datetime'),
                      on='datetime', direction='nearest',
                      tolerance=pd.Timedelta('3 hour'))
combo.dropna(axis=0, how='any', inplace=True)
ethane = ethane[ethane['datetime'] > dt.datetime(2019, 1, 1, 1)]

sns.set(style="whitegrid")
f, ax = plt.subplots(figsize=(9, 9))
sns.despine(f, left=True, bottom=True)
sns.scatterplot(x='datetime', y='val', data=combo, hue='# persons', ax=ax, s=70, zorder=5,
                palette='seismic',
                hue_norm=(0, 6))
sns.scatterplot(x='datetime', y='val', data=ethane, ax=ax, s=25, alpha=0.5, color='red', label='Background Values')
plt.title('TAWO Vistor Log Correlation')
plt.xlabel('')
plt.ylabel('Ethane Mixing Ratio (ppb)')

# New xticks plot
months = mdates.MonthLocator()              # Add tick every month
days = mdates.DayLocator(range(1, 32, 5))   # Add tick every 5th day in a month
monthFmt = mdates.DateFormatter('%b')       # Use abbreviated month name

# Add the locators to the axis
ax.xaxis.set_major_locator(months)
ax.xaxis.set_major_formatter(monthFmt)
ax.xaxis.set_minor_locator(days)

plt.xlim(dt.datetime(2019, 4, 30), dt.datetime(2019, 7, 1))
plt.legend()

plt.show()

print('debug point')
