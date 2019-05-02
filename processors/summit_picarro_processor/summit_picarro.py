from pathlib import Path
import datetime as dt
from datetime import datetime
import statistics as s
from collections import namedtuple

import pandas as pd

"""
Project-Wide TODO List:

# 1) Figure out time difference in epoch
# 	1.1) Make all dates tz-aware

# 2) Configure relationship between Datums and DataFiles
# 3) Configure relationship between CalEvents and Datums (and Datafiles?)
4) Update VOC plots per recommendations
5) Calibration event developmment and testing

"""

from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship

from summit_core import JDict

Base = declarative_base()

column_names = ['alarm_status', 'instrument_status', 'cavity_pressure', 'cavity_temp', 'das_temp', 'etalon_temp',
                'warmbox_temp', 'mpv_position', 'outlet_valve', 'co', 'co2_wet', 'co2', 'ch4_wet', 'ch4', 'h2o']

column_to_instance_names = {'alarm_status': 'ALARM_STATUS', 'instrument_status': 'INST_STATUS',
                            'cavity_pressure': 'CavityPressure', 'cavity_temp': 'CavityTemp', 'das_temp': 'DasTemp',
                            'etalon_temp': 'EtalonTemp', 'warmbox_temp': 'WarmBoxTemp',
                            'mpv_position': 'MPVPosition', 'outlet_valve': 'OutletValve', 'co': 'CO_sync',
                            'co2_wet': 'CO2_sync', 'co2': 'CO2_dry_sync', 'ch4_wet': 'CH4_sync',
                            'ch4': 'CH4_dry_sync', 'h2o': 'H2O_sync'}

mpv_converter = {1: 'ambient', 2: 'low_std', 4: 'mid_std', 3: 'high_std'}

standards = {'low_std': {'co': 69.6, 'co2': 390.24, 'ch4': 1838.5},
             'mid_std': {'co': 117.4, 'co2': 408.65, 'ch4': 1925.5},
             'high_std': {'co': 174.6, 'co2': 428.53, 'ch4': 2050.6}}

Point = namedtuple('Point', 'x y')
Curve = namedtuple('Point', 'm intercept')


# high_limit_dict = {'CH4': (2035, 2055, 2050.6),'CO2': (415, 435, 428.53),'CO': (160, 190, 174.6)}  # values are (high limit, low limit, cert value)
# mid_limit_dict = {'CH4': (1905, 1935, 1925.5),'CO2': (390, 410, 408.65),'CO': (100, 130, 117.4)}
# low_limit_dict = {'CH4': (1820, 1850, 1838.5),'CO2': (375, 395, 390.24),'CO': (60, 90, 69.6)}


class DataFile(Base):
    """
    A file containing synced 5-second data from the Picarro. Used mostly for tracking where data originated from for
    manual inspection, etc.
    """
    __tablename__ = 'files'

    id = Column(Integer, primary_key=True)
    _name = Column(String)
    _path = Column(String, unique=True)
    size = Column(Integer)
    processed = Column(Boolean)

    datum = relationship('Datum')

    def __init__(self, path):
        self.path = path
        self.size = Path.stat(path).st_size
        self.processed = False

    @property
    def path(self):
        return Path(self._path)

    @path.setter
    def path(self, value):
        self._path = str(value)
        self._name = value.name

    @property
    def name(self):
        return self._name


class Datum(Base):
    """
    A piece of 5-second data, originating from a file written by the Picarro. Most fields are preserved from the
    original files, but CO2 is changed to CO2_wet (etc), and the standard versions (CO2, CH4) are the dry measurements.
    """
    __tablename__ = 'data'

    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    alarm_status = Column(Integer)
    instrument_status = Column(Integer)
    cavity_pressure = Column(Float)
    cavity_temp = Column(Float)
    das_temp = Column(Float)
    etalon_temp = Column(Float)
    warmbox_temp = Column(Float)
    mpv_position = Column(Float)
    outlet_valve = Column(Float)
    co = Column(Float)
    co2_wet = Column(Float)
    co2 = Column(Float)
    ch4_wet = Column(Float)
    ch4 = Column(Float)
    h2o = Column(Float)

    file_id = Column(Integer, ForeignKey('files.id'))
    cal = relationship('CalEvent', back_populates='data')
    cal_id = Column(Integer, ForeignKey('cals.id'))

    def __init__(self, line_dict):
        for var in column_names:
            setattr(self, var, line_dict.get(column_to_instance_names.get(var)))

        self.date = datetime.utcfromtimestamp(line_dict.get('EPOCH_TIME'))


class CalEvent(Base):
    """
    A section of calibration data for a single standard or gas. These are related to their sub-data, but have result
    statistics, as well as a record of how they were made.
    """
    __tablename__ = 'cals'

    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    data = relationship('Datum', back_populates='cal')
    standard_used = Column(String)  # type = 'high' | 'mid' | 'low' | other_specific_names
    co_result = Column(MutableDict.as_mutable(JDict))  # {'mean': x, 'median': x, 'stdev': x}
    co2_result = Column(MutableDict.as_mutable(JDict))  # {'mean': x, 'median': x, 'stdev': x}
    ch4_result = Column(MutableDict.as_mutable(JDict))  # {'mean': x, 'median': x, 'stdev': x}
    back_period = Column(Float)  # seconds to look back when calculating result (whatever was used for last result)

    mastercal = relationship('MasterCal', back_populates='subcals')
    mastercal_id = Column(Integer, ForeignKey('mastercals.id'))

    def __init__(self, data, standard_used):
        self.data = data
        self.date = self.dates[-1]  # date of a CalEvent is the last timestamp in the cal period
        self.standard_used = standard_used

    def calc_result(self, compound, back_period):
        """
        Calculate the results for this standard for the given compound.

        :param compound: string, which compound to average?
        :param back_period: seconds to back-average from end of calibration period
        :return:
        """
        assert compound in ['co2', 'ch4', 'co'], "Compound not valid."

        dates = self.dates
        compound_data = getattr(self, compound)

        cutoff_date = self.date - dt.timedelta(seconds=back_period)

        def find_ind(dates, cutoff):
            """
            :param dates: list of datetimes (assumed to be sorted, increasing)
            :param cutoff: datetime to find values over
            :return: index where dates are greater than cutoff
            """
            for ind, date in enumerate(dates):
                if date > cutoff:
                    return ind - 1

            return None

        if compound_data is not None:
            ind = find_ind(dates, cutoff_date)
            if ind is not None:
                data_to_use = compound_data[ind:]
                result = {'mean': s.mean(data_to_use), 'median': s.median(data_to_use), 'stdev': s.stdev(data_to_use)}
            else:
                result = {'mean': None, 'median': None, 'stdev': None}

            setattr(self, compound + '_result', result)

        self.back_period = back_period
        return

    @property
    def dates(self):
        return [d.date for d in self.data]

    @property
    def co(self):
        return [d.co for d in self.data]

    @property
    def co2(self):
        return [d.co2 for d in self.data]

    @property
    def ch4(self):
        return [d.ch4 for d in self.data]

    @property
    def duration(self):
        return self.date - self.dates[0]


class MasterCal(Base):
    """
    A whole calibration, which consists of a high standard, low standard, and middle standard measurement.
    The three are joined into a MasterCal when all three have been run in sequence.

    A curve is calculated from the high/low values, then the middle's difference from it's expected value is used as a
    QC measure.

    As of 3/19/2019, it's not clear how these will be applied to the final data.
    """

    __tablename__ = 'mastercals'
    id = Column(Integer, primary_key=True)
    subcals = relationship('CalEvent', back_populates='mastercal')

    co_slope = Column(Float)
    co_intercept = Column(Float)
    co_middle_offset = Column(Float)
    co2_slope = Column(Float)
    co2_intercept = Column(Float)
    co2_middle_offset = Column(Float)
    ch4_slope = Column(Float)
    ch4_intercept = Column(Float)
    ch4_middle_offset = Column(Float)

    def __init__(self, standards):
        self.subcals = standards

    def create_curve(self):
        """
        Calculate a slope and intercept from the high/low values, and computer the y-difference from the curve to the
        middle value for QC.
        """
        for cpd in ['co', 'co2', 'ch4']:
            low_val = getattr(self.low_std, cpd + '_result').get('mean')
            high_val = getattr(self.high_std, cpd + '_result').get('mean')
            mid_val = getattr(self.mid_std, cpd + '_result').get('mean')

            low_coord = Point(standards.get('low_std').get(cpd),
                              low_val)  # (x, y) where x is the independent (certified value)
            mid_coord = Point(standards.get('mid_std').get(cpd), mid_val)
            high_coord = Point(standards.get('high_std').get(cpd), high_val)

            curve = calc_two_pt_curve(low_coord, high_coord)  # returns Curve(m, intercept) namedtuple
            setattr(self, cpd + '_slope', curve.m)
            setattr(self, cpd + '_intercept', curve.intercept)
            middle_y_offset = mid_coord.y - (curve.m * mid_coord.x + curve.intercept)
            # y offset is (actual y) - (expected y along the curve)
            # so a positive offset means the actual measurement was above the curve; negative below
            setattr(self, cpd + '_middle_offset', middle_y_offset)

    @property
    def high_std(self):
        return find_cal_by_type(self.subcals, 'high_std')

    @property
    def mid_std(self):
        return find_cal_by_type(self.subcals, 'mid_std')

    @property
    def low_std(self):
        return find_cal_by_type(self.subcals, 'low_std')


def find_cal_by_type(standards, std_type):
    """

    :param standards: list, of CalEvent objects
    :param std_type: string, in ['high', 'mid', 'low']
    :return: returns the first CalEvent in the list with the matching standard type
    """

    return next((ce for ce in standards if ce.standard_used == std_type), None)


def find_cal_indices(datetimes):
    """
    Cal events are any time a standard is injected and being quantified by the system. Here, they're separated as though
    any calibration data that's more than 10s away from the previous cal data is a new event.

    :param epoch_time: array of epoch times for all of the supplied data
    :return: list of cal events indices, where each index is the beginning of a new cal event
    """
    diff = datetimes.diff()
    indices = diff.loc[diff > pd.Timedelta(seconds=60)].index.values.tolist()  # subtract one from all indices
    return indices


def log_event_quantification(logger, event):
    """
    This condenses some repetitive logging behavior. Each time a CalEvent is created, this will log the results to the
    log as DEBUG (not console).
    :param logger: logger object from logging library
    :param event: CalEvent object, with new results
    :return: None, output to log file
    """
    logger.debug(f'CalEvent for date {event.date}, of duration {event.duration} quantified:')
    logger.debug('Result Sets Below (Mean, Median, StDev)')
    logger.debug(f'CO: {event.co_result["mean"]:.03f}, CO2: {event.co2_result["mean"]:.03f},' +
                 ' CH4: {event.ch4_result["mean"]:.03f}')
    logger.debug(f'CO: {event.co_result["median"]:.03f}, CO2: {event.co2_result["median"]:.03f},' +
                 ' CH4: {event.ch4_result["median"]:.03f}')
    logger.debug(f'CO: {event.co_result["stdev"]:.03f}, CO2: {event.co2_result["stdev"]:.03f},' +
                 ' CH4: {event.ch4_result["stdev"]:.03f}')

    return


def summit_picarro_plot(dates, compound_dict, limits=None, minor_ticks=None, major_ticks=None, unit_string='ppbv'):
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
            ax.plot(val_list[0], val_list[1], '-o')

    else:
        for compound, val_list in compound_dict.items():
            ax.plot(dates, val_list[1], '-o')

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
    ax.legend(compound_dict.keys())

    f1.subplots_adjust(bottom=.20)

    plot_name = f'{fn_list}_last_week.png'
    f1.savefig(plot_name, dpi=150)
    plt.close(f1)

    return plot_name


def match_cals_by_min(cal, cals, minutes=4):
    """
    :param cal: CalEvent, the one to be matched to from cals
    :param cals: list, of CalEvents
    :param minutes: int, minutes difference to tolerate ## MAY CHANGE TO upper/lower limits
    :return: cal from the list cals, or None
    """
    from summit_core import find_closest_date, search_for_attr_value

    cal_dates = [c.date for c in cals]

    if not cal_dates:
        return None

    [match, diff] = find_closest_date(cal.date, cal_dates)

    if not match or not diff:
        return None

    if abs(diff) < dt.timedelta(minutes=minutes):
        return search_for_attr_value(cals, 'date', match)  # return the matching cal if within tolerance
    else:
        return None


def calc_two_pt_curve(low, high):
    """
    :param low: Point namedtuple, (x,y)
    :param high: Point namedtuple, (x,y)
    :return: tuple, (float, float), (m, intercept)
    """

    m = (high.y - low.y) / (high.x - low.x)
    intercept = low.y - m * low.x

    return Curve(m, intercept)
