import os
import json
from pathlib import Path
from datetime import datetime

from sqlalchemy.types import TypeDecorator, VARCHAR
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, DateTime

Base = declarative_base()


def find_project_dir(runpath):
    """
    Scans up directories until it finds the project folder.
    :param runpath: pathlib.Path, where summit_core is called from.
    :return: pathlib.Path, the base project directory
    """
    runpath = runpath.resolve()

    if runpath.name == "Summit" or runpath.name == 'summit_master':
        return runpath
    else:
        runpath = runpath / '..'
        return find_project_dir(runpath)


project_dir = find_project_dir(Path(os.getcwd()))

voc_dir = project_dir / 'processors/summit_voc_processor'
picarro_dir = project_dir / 'processors/summit_picarro_processor'
methane_dir = project_dir / 'processors/summit_methane_processor'
error_dir = project_dir / 'processors/errors'
core_dir = project_dir / 'core'
taylor_basepath = '/data/web/htdocs/instaar/groups/arl/res_parameters/summit_plots'

processor_dirs = [voc_dir, picarro_dir, methane_dir, error_dir, core_dir]

data_file_paths = json.loads((core_dir / 'file_locations.json').read_text())

for k, v in data_file_paths.items():
    data_file_paths[k] = Path(v)  # Pathify stored string paths

methane_LOG_path = data_file_paths.get('methane_LOG')
methane_logs_path = data_file_paths.get('methane_logs')

voc_LOG_path = data_file_paths.get('voc_LOG')
voc_logs_path = data_file_paths.get('voc_logs')

picarro_logs_path = data_file_paths.get('picarro_logs')

taylor_auth = data_file_paths.get('taylor_server_auth')


class Config(Base):
    __tablename__ = 'config'

    id = Column(Integer, primary_key=True)

    processor = Column(String, unique=True)  # only one config per processor
    filesize = Column(Integer)
    pa_startline = Column(Integer)
    last_data_date = Column(DateTime)
    days_to_plot = Column(Integer)

    def __init__(self, processor=None, filesize=0, pa_startline=0, last_data_date=datetime(1900,1,1), days_to_plot=7):
        self.processor = processor
        self.filesize = filesize
        self.pa_startline = pa_startline
        self.last_data_date = last_data_date
        self.days_to_plot = days_to_plot


class TempDir:
    """
    Context manager for working in a directory.
    Pulled from: (https://pythonadventures.wordpress.com/2013/12/15/chdir-
                    a-context-manager-for-switching-working-directories/)
    """

    def __init__(self, path):
        self.old_dir = os.getcwd()
        self.new_dir = path

    def __enter__(self):
        os.chdir(self.new_dir)

    def __exit__(self, *args):
        os.chdir(self.old_dir)


class JDict(TypeDecorator):
    """
    Serializes a dictionary for SQLAlchemy storage.
    """
    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value


class JList(TypeDecorator):
    """
    Serializes a list for SQLAlchemy storage.
    """
    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        value = json.loads(value)
        return value


class Plot(Base):
    __tablename__ = 'plots'

    id = Column(Integer, primary_key=True)

    staged = Column(Boolean)
    _path = Column(String, unique=True)
    _name = Column(String)

    def __init__(self, path, staged):
        self.path = path
        self.staged = staged

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


MutableList.associate_with(JList)
MutableDict.associate_with(JDict)


def configure_logger(rundir, name):
    """
    Create the project-specific logger. DEBUG and up is saved to the log, INFO and up appears in the console.

    :param rundir: Path, to create log sub-path in
    :param name: str, name for logfile
    :return: logger object
    """
    from pathlib import Path
    import logging

    logfile = Path(rundir) / f'processor_logs/{name}.log'
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s -%(levelname)s- %(message)s')

    [H.setFormatter(formatter) for H in [ch, fh]]
    if not len(logger.handlers):
        _ = [logger.addHandler(H) for H in [ch, fh]]

    return logger


def connect_to_db(engine_str, directory):
    """
    Takes string name of the database to create/connect to, and the directory it should be in.

    :param engine_str: connection string for the database
    :param directory: directory the database should in (created?) in
    :return: engine, session, Base

    Example:
    engine, session, Base = connect_to_db('sqlite:///reservoir.sqlite', dir)
    """

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    # Base = declarative_base()  # needed to subclass for sqlalchemy objects

    with TempDir(directory):
        engine = create_engine(engine_str)
    sessy = sessionmaker(bind=engine)
    sess = sessy()

    return engine, sess


def check_filesize(filepath):
    """
    Returns the filesize in bytes.
    :param filepath: file-like object
    :return: int, filesize in bytes
    """
    import logging

    logger = logging.getLogger(__name__)

    if Path.is_file(filepath):
        return Path.stat(filepath).st_size
    else:
        logger.warning(f'File {filepath.name} not found.')
        return


def list_files_recur(path):
    """
    :param path: pathlib Path object
    :return: list, of file-like Path objects
    """
    files = []
    for file in path.rglob('*'):
        files.append(file)

    return files


def get_all_data_files(path, filetype):
    """
    Recursively search the given directory for .xxx files.

    :param path: Path to search
    :param filetype: str, ".type" of file to search for
    :return: list, of file-like Path objects
    """
    files = list_files_recur(path)
    files[:] = [file for file in files if filetype in file.name]

    return files


def search_for_attr_value(obj_list, attr, value):
    """
    Finds the first (not necesarilly the only) object in a list, where its
    attribute 'attr' is equal to 'value', returns None if none is found.
    :param obj_list: list, of objects to search
    :param attr: string, attribute to search for
    :param value: mixed types, value that should be searched for
    :return: obj, from obj_list, where attribute attr matches value
        **** warning: returns the *first* obj, not necessarily the only
    """
    return next((obj for obj in obj_list if getattr(obj, attr, None) == value), None)


def find_closest_date(date, list_of_dates):
    """
    This is a helper function that works on Python datetimes. It returns the closest date value,
    and the timedelta from the provided date.
    :param date: datetime
    :param list_of_dates: list, of datetimes
    :return: match, delta: the matching date from the list, and it's difference to the original as a timedelta
    """
    try:
        match = min(list_of_dates, key=lambda x: abs(x - date))
    except ValueError:
        return None, None

    delta = match - date

    return match, delta


def create_daily_ticks(days_in_plot):
    """
    Takes a number of days to plot back, and creates major (1 day) and minor (6 hour) ticks.

    :param days_in_plot: number of days to be displayed on the plot
    :return: date_limits, major_ticks, minor_ticks
    """
    from datetime import datetime
    import datetime as dt

    date_limits = dict()
    date_limits['right'] = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(
        days=1)  # end of day
    date_limits['left'] = date_limits['right'] - dt.timedelta(days=days_in_plot)

    major_ticks = [date_limits['right'] - dt.timedelta(days=x) for x in range(0, days_in_plot + 1)]
    minor_ticks = [date_limits['right'] - dt.timedelta(hours=x * 6) for x in range(0, days_in_plot * 4 + 1)]

    return date_limits, major_ticks, minor_ticks


def connect_to_sftp():
    import paramiko
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    server_info = json.loads(taylor_auth.read_text())
    client.connect(**server_info)
    return client.open_sftp()


def add_or_ignore_plot(plot, core_session):
    plots_in_db = core_session.query(Plot._path).all()

    if plot.path not in plots_in_db:
        core_session.add(plot)
    return


async def send_file_sftp(filepath):
    con = connect_to_sftp()
    con.chdir(taylor_basepath)
    con.put(str(filepath), filepath.name)
    con.close()
    return


async def check_send_plots(logger):
    try:
        from summit_errors import send_processor_email
    except ImportError as e:
        logger.error('ImportError occurred in check_send_plots()')
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_core.sqlite', core_dir)
    except Exception as e:
        logger.error(f'Exception {e.args} prevented connection to the database in check_send_plots()')
        send_processor_email('Core', exception=e)
        return False

    try:
        plots_to_upload = session.query(Plot).filter(Plot.staged == True).all()

        if plots_to_upload:
            for plot in plots_to_upload:
                await send_file_sftp(plot.path)
                logger.info(f'Plot {plot.name} uploaded to website.')
                session.delete(plot)
            session.commit()

        session.close()
        engine.dispose()
        return True

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in check_send_plots().')
        send_processor_email('Core', exception=e)
        session.close()
        engine.dispose()
        return False
