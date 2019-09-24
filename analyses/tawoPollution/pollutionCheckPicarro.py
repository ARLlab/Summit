import pandas as pd
from fileLoading import loadExcel, readCsv
import os
from dateConv import visitToDatetime, createDatetime
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
from pandas.plotting import register_matplotlib_converters
import matplotlib.dates as mdates # For formatting date
from sqlFuncs import get_data, create_connection

register_matplotlib_converters()

# import the data
root = r'C:\Users\ARL\Desktop\Jashan\Summit\analyses\Data'
datapath = os.path.join(root, 'TAWO_visit_log.xlsx')
visits = loadExcel(datapath)
root2 = r'C:\Users\ARL\Desktop\Jashan\Data'
dbdir = os.path.join(root2, 'summit_picarro.sqlite')

conn = create_connection(dbdir)

# identify date range
start = '2019-06-00 00:00:00'
end = '2019-07-30 23:59:59'

# get the data
with conn:
    methane = get_data(conn, start, end, 'data')

# data cleaning
dates = visits['Date'].values
dates = dates[1:]
badcols = ['Initials', 'Unnamed: 5', 'Date']
visits.drop(badcols, axis=1, inplace=True)
visits.drop([0], axis=0, inplace=True)
visits.dropna(axis=0, how='all', inplace=True)
visits.reset_index(drop=True, inplace=True)


# create proper datetimes
visits['start'], visits['end'] = visitToDatetime(dates,
                                                 visits['Arrival time (Z)'].values,
                                                 visits['Departure time (Z)'].values)

# methane cleaning
methane.dropna(how='any', inplace=True)
methane.reset_index(drop=True, inplace=True)
methane = methane[methane['status'] == 963]
methane = methane[methane['pos'] == 1.0]

# remove leftover columns
badcols = ['Arrival time (Z)', 'Departure time (Z)']
visits.drop(badcols, axis=1, inplace=True)

# calculate middle date time
visits['datetime'] = visits['start'] + (visits['end'] - visits['start'])/2


combo = pd.merge_asof(visits.sort_values('datetime'),
                      methane.sort_values('datetime'),
                      on='datetime', direction='nearest',
                      tolerance=pd.Timedelta('1 minute'))
combo.dropna(axis=0, how='any', inplace=True)
methane = methane[methane['datetime'] > dt.datetime(2019, 1, 1, 1)]
combo.drop([21, 22, 31], axis=0, inplace=True)
combo.reset_index(drop=True, inplace=True)

max = 1950
min = 1890
rangey = max-min

sns.set(style="whitegrid")
f, ax = plt.subplots(figsize=(9, 9))
sns.despine(f, left=True, bottom=True)
for i in range(len(combo)):
    ax.axvspan(xmin=combo['start'].iloc[i],
               xmax=combo['end'].iloc[i],
               ymin=((combo['ch4'].iloc[i] - min) / rangey) - 0.1,
               ymax=((combo['ch4'].iloc[i] - min) / rangey) + 0.1,
               alpha=0.3, color='red')
sns.scatterplot(x='datetime', y='ch4', data=methane, ax=ax, s=25, alpha=0.99, color='blue', label='Background Values')
plt.title('TAWO Vistor Log Correlation')
plt.xlabel('')
plt.ylabel('Methane Mixing Ratio (ppb) [Picarro Data]')

# New xticks plot
months = mdates.MonthLocator()              # Add tick every month
monthFmt = mdates.DateFormatter('%b')       # Use abbreviated month name

# Add the locators to the axis
ax.xaxis.set_major_locator(months)
ax.xaxis.set_major_formatter(monthFmt)

plt.xlim(dt.datetime(2019, 6, 1), dt.datetime(2019, 7, 31))
plt.ylim(min, max)
plt.legend()

plt.show()
