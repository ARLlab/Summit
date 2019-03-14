import os, logging, json
from pathlib import Path
import datetime as dt
from datetime import datetime

"""
Project-Wide TODO List:

# 1) Figure out time difference in epoch
# 	1.1) Make all dates tz-aware

2) Configure relationship between Datums and DataFiles
3) Configure relationship between CalEvents and Datums (and Datafiles?)
4) Update VOC plots per recommendations
5) Calibration event developmment and testing

"""

from sqlalchemy.types import TypeDecorator, VARCHAR
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()

# rundir = Path(r'C:\Users\arl\Desktop\summit_master\processors\summit_picarro_processor')
rundir = Path(r'C:\Users\brend\PycharmProjects\Summit\processors\summit_picarro_processor')
# rundir = Path(os.getcwd())

column_names = ['alarm_status', 'instrument_status', 'cavity_pressure', 'cavity_temp', 'das_temp', 'etalon_temp',
				'warmbox_temp', 'mpv_position', 'outlet_valve', 'co', 'co2_wet', 'co2', 'ch4_wet', 'ch4', 'h2o']

column_to_instance_names = {'alarm_status': 'ALARM_STATUS', 'instrument_status': 'INST_STATUS',
							'cavity_pressure': 'CavityPressure', 'cavity_temp': 'CavityTemp', 'das_temp': 'DasTemp',
							'etalon_temp': 'EtalonTemp', 'warmbox_temp': 'WarmBoxTemp',
							'mpv_position': 'MPVPosition', 'outlet_valve': 'OutletValve', 'co': 'CO_sync',
							'co2_wet': 'CO2_sync', 'co2': 'CO2_dry_sync', 'ch4_wet': 'CH4_sync',
							'ch4': 'CH4_dry_sync', 'h2o': 'H2O_sync'}


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


MutableList.associate_with(JList)
MutableDict.associate_with(JDict)


class TempDir():
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


class DataFile(Base):

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

	def __init__(self, line_dict):

		for var in column_names:
			setattr(self, var, line_dict.get(column_to_instance_names.get(var)))

		self.date = datetime.utcfromtimestamp(line_dict.get('EPOCH_TIME'))


class CalEvent():

	__tablename__ = 'calibrations'

	id = Column(Integer, primary_key=True)
	date = Column(DateTime)
	co = Column(MutableList.as_mutable(JList))
	co2 = Column(MutableList.as_mutable(JList))
	ch4 = Column(MutableList.as_mutable(JList))

	co_result = Column(Float)
	co2_result = Column(Float)
	ch4_result = Column(Float)

	# TODO: Needs relationship to it's child Datums

	def __init__(self):
		pass


def connect_to_db(engine_str, directory):
	"""
	Example:
	engine, session, Base = connect_to_db('sqlite:///reservoir.sqlite', dir)

	Takes string name of the database to create/connect to, and the directory it should be in.

	engine_str: str, name of the database to create/connect to.
	directory: str/path, directory that the database should be made/connected to in.
		Requires context manager TempDir in order to work with async
	"""

	from summit_picarro import Base, TempDir

	from sqlalchemy import create_engine
	from sqlalchemy.orm import sessionmaker

	with TempDir(directory):
		engine = create_engine(engine_str)
	Session = sessionmaker(bind=engine)
	sess = Session()

	return engine, sess, Base


def configure_logger(rundir):
	logfile = Path(rundir) / 'processor_logs/summit_picarro.log'
	logger = logging.getLogger('summit_voc')
	logger.setLevel(logging.DEBUG)
	fh = logging.FileHandler(logfile)
	fh.setLevel(logging.DEBUG)

	ch = logging.StreamHandler()
	ch.setLevel(logging.INFO)

	formatter = logging.Formatter('%(asctime)s -%(levelname)s- %(message)s')

	[H.setFormatter(formatter) for H in [ch, fh]]
	[logger.addHandler(H) for H in [ch, fh]]

	return logger


logger = configure_logger(rundir)


def check_filesize(filepath):
	'''Returns filesize in bytes'''
	if Path.is_file(filepath):
		return Path.stat(filepath).st_size
	else:
		logger.warning(f'File {filepath.name} not found.')
		return


def list_files_recur(path):
	files = []
	for file in path.rglob('*'):
		files.append(file)

	return files


def get_all_data_files(path):
	"""
	Recursively search the
	:param rundir_path:
	:return:
	"""
	files = list_files_recur(path)
	files[:] = [file for file in files if '.dat' in file.name]

	return files



