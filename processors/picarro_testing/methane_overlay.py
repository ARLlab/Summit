"""
This script overlays the Picarro CH4 Plots with the GC CH4 Plots, only for a given static timestep for demonstration
purposes. Will be modified to incorporated into the automatic near real-time processes according to response to
usefullness of example plot

"""

# Import libraries & functions
import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure

# Import Required Functions
from summit_core import connect_to_db
from summit_picarro import Base, Datum
from summit_methane import GcRun


# Create this function directly in here to adjust the date_limits to be set
def custom_create_daily_ticks(days_in_plot, minors_per_day=4):
    """
    Takes a number of days to plot back, and creates major (1 day) and minor (6 hour) ticks.

    :param days_in_plot: int, number of days to be displayed on the plot
    :param minors_per_day: int, number of minor ticks per day
    :return: date_limits, major_ticks, minor_ticks
    """

    import datetime as dt
    from datetime import datetime

    date_limits = dict()
    date_limits['right'] = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(
        days=1) - dt.timedelta(days=8)  # end of day
    date_limits['left'] = date_limits['right'] - dt.timedelta(days=days_in_plot)

    major_ticks = [date_limits['right'] - dt.timedelta(days=x) for x in range(0, days_in_plot + 1)]

    minor_ticks = [date_limits['right'] - dt.timedelta(hours=x * (24 / minors_per_day))
                   for x in range(0, days_in_plot * minors_per_day + 1)]

    return date_limits, major_ticks, minor_ticks


# Custom Version outputs the new plot
def custom_methane_plot(dates, compound_dict, limits=None, minor_ticks=None, major_ticks=None,
                              unit_string='ppbv'):
    """
    :param dates: list, of Python datetimes; if set, this applies to all compounds.
        If None, each compound supplies its own date values
    :param compound_dict: dict, {'compound_name':[dates, mrs]}
        keys: str, the name to be plotted and put into filename
        values: list, len(list) == 2, two parallel lists that are...
            dates: list, of Python datetimes. If None, dates come from dates input parameter (for all compounds)
            mrs: list, of [int/float/None]s; these are the mixing ratios to be plotted
    :param limits: dict, optional dictionary of limits including ['top','bottom','right','left']
    :param minor_ticks: list, of major tick marks
    :param major_ticks: list, of minor tick marks
    :param unit_string: string, will be displayed in y-axis label as f'Mixing Ratio ({unit_string})'
    :return: None

    This plots stuff.

    Example with all dates supplied:
        plot_last_week((None, {'Ethane':[[date, date, date], [1, 2, 3]],
                                'Propane':[[date, date, date], [.5, 1, 1.5]]}))

    Example with single date list supplied:
        plot_last_week([date, date, date], {'ethane':[None, [1, 2, 3]],
                                'propane':[None, [.5, 1, 1.5]]})
    """

    import matplotlib.pyplot as plt
    from matplotlib.dates import DateFormatter
    from pandas.plotting import register_matplotlib_converters
    register_matplotlib_converters()

    f1 = plt.figure()
    ax = f1.gca()

    if dates is None:  # dates supplied by individual compounds
        for compound, val_list in compound_dict.items():
            assert val_list[0] is not None, 'A supplied date list was None'
            assert (len(val_list[0]) > 0 and len(val_list[0]) == len(val_list[1])), \
                'Supplied dates were empty or lengths did not match'
            ax.plot(val_list[0], val_list[1], '-o', label='Picarro Methane')
            ax.plot(val_list[2], val_list[3], '-o', label='GC Methane')

    else:
        for compound, val_list, gc_list in compound_dict.items():
            ax.plot(dates, val_list[1], '-o', label='Picarro Methane')
            ax.plot(dates, val_list[3], '-o', label='GC Methane')


    compounds_safe = []
    for k, _ in compound_dict.items():
        """Create a filename-safe list using the given legend items"""
        compounds_safe.append(k.replace('-', '_').replace('/', '_').lower())

    comp_list = ', '.join(compound_dict.keys())  # use real names for plot title
    fn_list = '_'.join(compounds_safe).replace(' ', '_')  # use 'safe' names for filename

    if limits is not None:
        ax.set_xlim(right=limits.get('right'))
        ax.set_xlim(left=limits.get('left'))
        ax.set_ylim(top=limits.get('top'))
        ax.set_ylim(bottom=limits.get('bottom'))

    if major_ticks is not None:
        ax.set_xticks(major_ticks, minor=False)
    if minor_ticks is not None:
        ax.set_xticks(minor_ticks, minor=True)

    date_form = DateFormatter("%Y-%m-%d")
    ax.xaxis.set_major_formatter(date_form)

    [i.set_linewidth(2) for i in ax.spines.values()]
    ax.tick_params(axis='x', labelrotation=30)
    ax.tick_params(axis='both', which='major', size=8, width=2, labelsize=15)
    f1.set_size_inches(11.11, 7.406)

    ax.set_ylabel(f'Mixing Ratio ({unit_string})', fontsize=20)
    ax.set_title(f'{comp_list}', fontsize=24, y=1.02)
    ax.legend()

    f1.subplots_adjust(bottom=.20)

    plot_name = f'{fn_list}_last_week.png'
    f1.savefig(plot_name, dpi=150)
    plt.close(f1)

    return plot_name       # wanted to return the figure to add to it with methane


# Connect to the Picarro DB
rundir = r'C:\Users\ARL\Desktop\Testing DB'                                     # location of DB
engine, session = connect_to_db('sqlite:///Jsummit_picarro.sqlite', rundir)     # Create eng & sess
Base.metadata.create_all(engine)                                                # Create base

date_limits, major_ticks, minor_ticks = custom_create_daily_ticks(6)
all_data = (session.query(Datum.date, Datum.ch4)                                    # get date and methane
            .filter((Datum.mpv_position == 0.0) | (Datum.mpv_position == 1.0))      # filter for not cal events
            .filter((Datum.instrument_status == 963), (Datum.alarm_status == 0))    # filter out bad data
            .filter(Datum.date >= date_limits['left'])                                # just get certain dates
            .all())

# Gather the Picarro Methane Data
picarro_dates = []
picarro_ch4 = []
for result in all_data:
    picarro_dates.append(result.date)
    picarro_ch4.append(result.ch4)

# Connect to the Methane Database
engine, session = connect_to_db('sqlite:///Jsummit_methane.sqlite', rundir)     # Create eng & sess
Base.metadata.create_all(engine)                                                # Create base

# Gather GC Methane Data
runs_with_medians = (session.query(GcRun)
                     .filter(GcRun.median != None)
                     .filter(GcRun.standard_rsd < .02)
                     .filter(GcRun.rsd < .02)
                     .order_by(GcRun.date)
                     .all())

# Setup for Plotting
gc_dates = [run.date for run in runs_with_medians]
gc_ch4 = [run.median for run in runs_with_medians]


# Call the plotting function
graph = custom_methane_plot(None, ({'Summit Methane [Picarro & GC]': [picarro_dates, picarro_ch4, gc_dates, gc_ch4]}),
                           limits={'right': date_limits.get('right', None),
                                   'left': date_limits.get('left', None),
                                   'bottom': 1850,
                                   'top': 2050},
                           major_ticks=major_ticks,
                           minor_ticks=minor_ticks)

# Display the png
from PIL import Image
img = Image.open(graph)
img.show()



