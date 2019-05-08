from pathlib import Path
from datetime import datetime

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float

from summit_errors import send_processor_email

Base = declarative_base()

PROC = 'Daily Processor'

daily_parameters = ['date', 'ads_xfer_a', 'ads_xfer_b', 'valves_temp', 'gc_xfer_temp', 'cj1', 'catalyst', 'molsieve_a',
                    'molsieve_b', 'inlet_long', 'inlet_short', 'std_temp', 'cj2', 'battv', 'v12a', 'v12b', 'v15a',
                    'v15b', 'v24', 'v5a', 'mfc1', 'mfc4', 'mfc2', 'mfc5', 'mfc3a', 'mfc3b', 'h2_gen_p', 'line_p',
                    'zero_p','fid_p']


class Daily(Base):
    __tablename__ = 'dailies'

    id = Column(Integer, primary_key=True)

    _path = Column(String)
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

    @property
    def path(self):
        return Path(self._path)

    @path.setter
    def path(self, value):
        self._path = str(value.resolve())


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

    for daily in dailies:
        daily.path = filepath  # set filepath of all dailies at once

    return dailies


def summit_daily_plot():
    pass

    #maybe map a parameter object that has the parameter and limits to it? Some hard-coding seems inevitable.


async def check_load_dailies(logger):
    """
    TODO:

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

    try:
        from summit_core import connect_to_db, get_all_data_files, core_dir, daily_logs_path
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

        daily_files_in_db = session.query(Daily).all()
        daily_names_in_db = [file.path for file in daily_files_in_db]

        daily_files = get_all_data_files(daily_logs_path, '.txt')

        dailies = []
        for file in daily_files:
            filepath = Path(file)
            if filepath not in daily_names_in_db:
                new_dailies = read_daily_file(filepath)

                for n in new_dailies:
                    dailies.append(n)

        for daily in dailies:
            session.add(daily)

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


async def plot_dailies(logger):
    """
    TODO:

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

    try:
        from summit_core import connect_to_db, core_dir
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
        logger.info('Running plot_dailies()')

        # TODO

        session.close()
        engine.dispose()
        return True

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in plot_dailies()')
        send_processor_email(PROC, exception=e)
        session.close()
        engine.dispose()
        return False