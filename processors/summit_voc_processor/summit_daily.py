import os
from pathlib import Path
from datetime import datetime

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float, ForeignKey

from summit_errors import send_processor_email

Base = declarative_base()

PROC = 'Daily Processor'

daily_parameters = ['date', 'ads_xfer_a', 'ads_xfer_b', 'valves_temp', 'gc_xfer_temp', 'cj1', 'catalyst', 'molsieve_a',
                    'molsieve_b', 'inlet_long', 'inlet_short', 'std_temp', 'cj2', 'battv', 'v12a', 'v12b', 'v15a',
                    'v15b', 'v24', 'v5a', 'mfc1', 'mfc4', 'mfc2', 'mfc5', 'mfc3a', 'mfc3b', 'h2_gen_p', 'line_p',
                    'zero_p','fid_p']

class DailyFile(Base):
    __tablename__ = 'files'

    id = Column(Integer, primary_key=True)

    entries = relationship('Daily', back_populates='file')

    _path = Column(String, unique=True)
    _name = Column(String)
    size = Column(Integer)

    def __init__(self, path):
        self.path = path
        self.size = path.stat().st_size
        self.entries = []

    @property
    def path(self):
        return Path(self._path)

    @path.setter
    def path(self, value):
        self._path = str(value.resolve())
        self._name = value.name

    @property
    def name(self):
        return self._name


class Daily(Base):
    __tablename__ = 'dailies'

    id = Column(Integer, primary_key=True)

    file_id = Column(Integer, ForeignKey('files.id'))
    file = relationship('DailyFile', back_populates='entries')

    date = Column(DateTime)
    ads_xfer_a = Column(Float)
    ads_xfer_b = Column(Float)
    valves_temp = Column(Float)
    gc_xfer_temp = Column(Float)
    cj1 = Column(Float)
    catalyst = Column(Float)
    molsieve_a = Column(Integer)
    molsieve_b = Column(Integer)
    inlet_long = Column(Float)
    inlet_short = Column(Float)
    std_temp = Column(Float)
    cj2 = Column(Float)
    battv = Column(Float)
    v12a = Column(Float)
    v12b = Column(Float)
    v15a = Column(Float)
    v15b = Column(Float)
    v24 = Column(Float)
    v5a = Column(Float)
    mfc1 = Column(Float)
    mfc4 = Column(Float)
    mfc2 = Column(Float)
    mfc5 = Column(Float)
    mfc3a = Column(Float)
    mfc3b = Column(Float)
    h2_gen_p = Column(Float)
    line_p = Column(Float)
    zero_p = Column(Float)
    fid_p = Column(Float)

    def __init__(self, date, ads_xfer_a, ads_xfer_b, valves_temp, gc_xfer_temp, cj1, catalyst, molsieve_a, molsieve_b,
                 inlet_long, inlet_short, std_temp, cj2, battv, v12a, v12b, v15a, v15b, v24, v5a, mfc1, mfc4, mfc2,
                 mfc5, mfc3a, mfc3b, h2_gen_p, line_p, zero_p, fid_p):
        self.date = date
        self.ads_xfer_a = ads_xfer_a
        self.ads_xfer_b = ads_xfer_b
        self.valves_temp = valves_temp
        self.gc_xfer_temp = gc_xfer_temp
        self.cj1 = cj1
        self.catalyst = catalyst
        self.molsieve_a = molsieve_a
        self.molsieve_b = molsieve_b
        self.inlet_long = inlet_long
        self.inlet_short = inlet_short
        self.std_temp = std_temp
        self.cj2 = cj2
        self.battv = battv
        self.v12a = v12a
        self.v12b = v12b
        self.v15a = v15a
        self.v15b = v15b
        self.v24 = v24
        self.v5a = v5a
        self.mfc1 = mfc1
        self.mfc4 = mfc4
        self.mfc2 = mfc2
        self.mfc5 = mfc5
        self.mfc3a = mfc3a
        self.mfc3b = mfc3b
        self.h2_gen_p = h2_gen_p
        self.line_p = line_p
        self.zero_p = zero_p
        self.fid_p = fid_p


def read_daily_line(line):
    ls = line.split('\t')

    dailydata = {}

    dailydata['date'] = datetime.strptime(ls[0].split('.')[0], '%y%j%H%M')
    dailydata['ads_xfer_a'] = float(ls[1])
    dailydata['ads_xfer_b'] = float(ls[2])
    dailydata['valves_temp'] = float(ls[3])
    dailydata['gc_xfer_temp'] = float(ls[4])
    dailydata['cj1'] = float(ls[5])
    dailydata['catalyst'] = float(ls[6])
    dailydata['molsieve_a'] = float(ls[7])
    dailydata['molsieve_b'] = float(ls[8])
    dailydata['inlet_long'] = float(ls[9])
    dailydata['inlet_short'] = float(ls[10])
    dailydata['std_temp'] = float(ls[11])
    dailydata['cj2'] = float(ls[12])
    dailydata['battv'] = float(ls[13])
    dailydata['v12a'] = float(ls[14])
    dailydata['v12b'] = float(ls[15])
    dailydata['v15a'] = float(ls[16])
    dailydata['v15b'] = float(ls[17])
    dailydata['v24'] = float(ls[18])
    dailydata['v5a'] = float(ls[19])
    dailydata['mfc1'] = float(ls[20])
    dailydata['mfc4'] = float(ls[21])
    dailydata['mfc2'] = float(ls[22])
    dailydata['mfc5'] = float(ls[23])
    dailydata['mfc3a'] = float(ls[24])
    dailydata['mfc3b'] = float(ls[25])
    dailydata['h2_gen_p'] = float(ls[26])
    dailydata['line_p'] = float(ls[27])
    dailydata['zero_p'] = float(ls[28])
    dailydata['fid_p'] = float(ls[29])

    return dailydata


def read_daily_file(filepath):

    contents = filepath.read_text().split('\n')
    contents = [line for line in contents if line]

    dailies = []
    for line in contents:
        dailies.append(Daily(**read_daily_line(line)))

    return dailies


async def check_load_dailies(logger):
    """
    TODO:

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

    try:
        from summit_core import connect_to_db, get_all_data_files, core_dir, daily_logs_path, search_for_attr_value
    except ImportError as e:
        logger.error(f'ImportError occurred in check_load_dailies()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_daily.sqlite', core_dir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the database in check_load_dailies()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running check_load_dailies()')

        daily_files_in_db = session.query(DailyFile).all()

        daily_files = [DailyFile(path) for path in get_all_data_files(daily_logs_path, '.txt')]

        new_files = []

        for file in daily_files:
            file_in_db = search_for_attr_value(daily_files_in_db, 'path', file.path)

            if not file_in_db:
                new_files.append(file)
                logger.info(f'File {file.name} added for processing.')
            else:
                if file.size > file_in_db.size:
                    logger.info(f'File {file_in_db.name} added to process additional data.')
                    new_files.append(file_in_db)

        if new_files:
            for file in new_files:
                dailies = read_daily_file(file.path)
                file_daily_dates = [d.date for d in file.entries]
                file.entries.extend([d for d in dailies if d.date not in file_daily_dates])
                file.size = file.path.stat().st_size
                session.merge(file)

            session.commit()

        session.close()
        engine.dispose()
        return True

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in check_load_dailies()')
        send_processor_email(PROC, exception=e)
        session.close()
        engine.dispose()
        return False


def summit_daily_plot(dates, compound_dict, limits=None, minor_ticks=None, major_ticks=None,
                    y_label_str='Temperature (\xb0C)'):
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
            if val_list[0] and val_list[1]:
                assert len(val_list[0]) > 0 and len(val_list[0]) == len(
                    val_list[1]), 'Supplied dates were empty or lengths did not match'
                ax.plot(val_list[0], val_list[1], '-o')
            else:
                pass

    else:
        for compound, val_list in compound_dict.items():
            ax.plot(dates, val_list[1], '-o')

    compounds_safe = []
    for k, _ in compound_dict.items():
        """Create a filename-safe list using the given legend items"""
        compounds_safe.append(k.replace('-', '_')
                                .replace('/', '_')
                                .replace(' ', '_').lower())

    comp_list = ', '.join(compound_dict.keys())  # use real names for plot title
    fn_list = '_'.join(compounds_safe)  # use 'safe' names for filename

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

    ax.set_ylabel(y_label_str, fontsize=20)
    ax.set_title(f'{comp_list}', fontsize=24, y=1.02)
    ax.legend(compound_dict.keys())

    f1.subplots_adjust(bottom=.20)

    plot_name = f'{fn_list}.png'
    f1.savefig(plot_name, dpi=150)
    plt.close(f1)

    return plot_name


async def plot_dailies(logger):
    """
    Loads dailies for the last 3 weeks and plots with ticks for every three days and minor ticks for every day.
    Plots are registered with the core database so they're uploaded to the Taylor drive.

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

    try:
        from pathlib import Path
        import datetime as dt
        from summit_core import connect_to_db, core_dir, TempDir, Config, Plot, add_or_ignore_plot, create_daily_ticks
        plotdir = core_dir / 'plots/daily'
        remotedir = r'/data/web/htdocs/instaar/groups/arl/summit/protected/plots'

        try:
            os.chdir(plotdir)
        except FileNotFoundError:
            os.mkdir(plotdir)

    except ImportError as e:
        logger.error(f'ImportError occurred in plot_dailies()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_daily.sqlite', core_dir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the database in plot_dailies()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        core_engine, core_session = connect_to_db('sqlite:///summit_core.sqlite', core_dir)
        Plot.__table__.create(core_engine, checkfirst=True)
        Config.__table__.create(core_engine, checkfirst=True)

        daily_config = core_session.query(Config).filter(Config.processor == PROC).one_or_none()

        if not daily_config:
            daily_config = Config(processor=PROC, days_to_plot=21)  # use all default values except processor on init
            core_session.add(daily_config)
            core_session.commit()

    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the core database in plot_new_data()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running plot_dailies()')

        date_ago = datetime.now() - dt.timedelta(
            days=daily_config.days_to_plot + 1)  # set a static for retrieving data at beginning of plot cycle

        date_limits, major_ticks, minor_ticks = create_daily_ticks(daily_config.days_to_plot, minors_per_day=1)

        major_ticks = [t for ind, t in enumerate(major_ticks) if ind % 3 == 0]  # use every third daily tick

        dailies = session.query(Daily).filter(Daily.date >= date_ago).order_by(Daily.date).all()

        dailydict = {}
        for param in daily_parameters:
            dailydict[param] = [getattr(d, param) for d in dailies]

        with TempDir(plotdir):  ## PLOT i-butane, n-butane, acetylene

            name = summit_daily_plot(dailydict.get('date'), ({'Ads Xfer A': [None, dailydict.get('ads_xfer_a')],
                                                              'Ads Xfer B': [None, dailydict.get('ads_xfer_b')],
                                                              'Valves Temp': [None, dailydict.get('valves_temp')],
                                                              'GC Xfer Temp': [None, dailydict.get('gc_xfer_temp')],
                                                              'Catalyst': [None, dailydict.get('catalyst')]}),
                                     limits={'right': date_limits.get('right', None),
                                             'left': date_limits.get('left', None),
                                             'bottom': 0,
                                             'top': 475},
                                     major_ticks=major_ticks,
                                     minor_ticks=minor_ticks)

            hot_plot = Plot(plotdir / name, remotedir, True)
            add_or_ignore_plot(hot_plot, core_session)

            name = summit_daily_plot(dailydict.get('date'), ({'CJ1 Temp': [None, dailydict.get('cj1')],
                                                              'CJ2 Temp': [None, dailydict.get('cj2')],
                                                              'Standard Temp': [None, dailydict.get('std_temp')]}),
                                     limits={'right': date_limits.get('right', None),
                                             'left': date_limits.get('left', None),
                                             'bottom': 10,
                                             'top': 50},
                                     major_ticks=major_ticks,
                                     minor_ticks=minor_ticks)

            room_plot = Plot(plotdir / name, remotedir, True)
            add_or_ignore_plot(room_plot, core_session)

            name = summit_daily_plot(dailydict.get('date'), ({'H2 Gen Pressure': [None, dailydict.get('h2_gen_p')],
                                                              'Line Pressure': [None, dailydict.get('line_p')],
                                                              'Zero Pressure': [None, dailydict.get('zero_p')],
                                                              'FID Pressure': [None, dailydict.get('fid_p')]}),
                                     limits={'right': date_limits.get('right', None),
                                             'left': date_limits.get('left', None),
                                             'bottom': 0,
                                             'top': 75},
                                     y_label_str='Pressure (PSI)',
                                     major_ticks=major_ticks,
                                     minor_ticks=minor_ticks)

            pressure_plot = Plot(plotdir / name, remotedir, True)
            add_or_ignore_plot(pressure_plot, core_session)

            name = summit_daily_plot(dailydict.get('date'),
                                     ({'Inlet Short Temp': [None, dailydict.get('inlet_short')]}),
                                     limits={'right': date_limits.get('right', None),
                                             'left': date_limits.get('left', None),
                                             'bottom': 0,
                                             'top': 60},
                                     major_ticks=major_ticks,
                                     minor_ticks=minor_ticks)

            inlet_plot = Plot(plotdir / name, remotedir, True)
            add_or_ignore_plot(inlet_plot, core_session)

            name = summit_daily_plot(dailydict.get('date'), ({'Battery V': [None, dailydict.get('battv')],
                                                              '12Va': [None, dailydict.get('v12a')],
                                                              '15Va': [None, dailydict.get('v15a')],
                                                              '15Vb': [None, dailydict.get('v15b')],
                                                              '24V': [None, dailydict.get('v24')],
                                                              '5Va': [None, dailydict.get('v5a')]}),
                                     limits={'right': date_limits.get('right', None),
                                             'left': date_limits.get('left', None),
                                             'bottom': 0,
                                             'top': 30},
                                     y_label_str='Voltage (v)',
                                     major_ticks=major_ticks,
                                     minor_ticks=minor_ticks)

            voltage_plot = Plot(plotdir / name, remotedir, True)
            add_or_ignore_plot(voltage_plot, core_session)

            name = summit_daily_plot(dailydict.get('date'), ({'MFC1': [None, dailydict.get('mfc1')],
                                                              'MFC2': [None, dailydict.get('mfc2')],
                                                              'MFC3a': [None, dailydict.get('mfc3a')],
                                                              'MFC3b': [None, dailydict.get('mfc3b')],
                                                              'MFC4': [None, dailydict.get('mfc4')],
                                                              'MFC5': [None, dailydict.get('mfc5')]}),
                                     limits={'right': date_limits.get('right', None),
                                             'left': date_limits.get('left', None),
                                             'bottom': -1,
                                             'top': 3.5},
                                     y_label_str='Flow (Ml/min)',
                                     major_ticks=major_ticks,
                                     minor_ticks=minor_ticks)

            flow_plot = Plot(plotdir / name, remotedir, True)
            add_or_ignore_plot(flow_plot, core_session)

        core_session.commit()
        core_session.close()
        core_engine.dispose()

        session.close()
        engine.dispose()
        return True

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in plot_dailies()')
        send_processor_email(PROC, exception=e)
        session.close()
        engine.dispose()
        return False