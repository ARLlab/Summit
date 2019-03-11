import os, logging
from pathlib import Path
import datetime as dt
from datetime import datetime

from sqlalchemy.types import TypeDecorator, VARCHAR
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()

rundir = Path(r'C:\Users\arl\Desktop\summit_master\processors\summit_picarro_processor')
# rundir = Path(os.getcwd())

column_names = ['alarm_status', 'instrument_status', 'cavity_pressure', 'cavity_temp', 'das_temp', 'etalon_temp', 'warmbox_temp', 'mpv_position', 'outlet_valve', 'CO', 'CO2', 'CO2_dry', 'CH4', 'CH4_dry', 'H2O']

column_to_instance_names = {'alarm_status': 'ALARM_STATUS', 'instrument_status': 'INST_STATUS',
								'cavity_pressure': 'CavityPressure', 'cavity_temp': 'CavityTemp', 'das_temp': 'DasTemp',
								'etalon_temp': 'EtalonTemp', 'warmbox_temp': 'WarmBoxTemp',
								'mpv_position': 'MPVPosition', 'outlet_valve': 'OutletValve', 'CO': 'CO_sync',
								'CO2': 'CO2_sync', 'CO2_dry': 'CO2_dry_sync', 'CH4': 'CH4_sync',
								'CH4_dry': 'CH4_dry_sync', 'H2O': 'H2O_sync'}


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
	_path = Column(String)
	size = Column(Integer)

	def __init__(self, path):
		self.path = path
		self.size = Path.stat(path).st_size

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
	CO = Column(Float)
	CO2 = Column(Float)
	CO2_dry = Column(Float)
	CH4 = Column(Float)
	CH4_dry = Column(Float)
	H2O = Column(Float)

	def __init__(self, line_dict):

		for var in column_names:
			setattr(self, var, line_dict.get(column_to_instance_names.get(var)))

		self.date = datetime.fromtimestamp(line_dict.get('EPOCH_TIME'))


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


def list_files_recur(path):
	files = []
	for file in path.rglob('*'):
		files.append(file)

	return files


def get_all_data_files(rundir_path):
	"""
	Recursively search the
	:param rundir_path:
	:return:
	"""
	data_path = rundir_path / 'data'
	files = list_files_recur(data_path)
	files[:] = [file for file in files if '.dat' in file.name]

	return files



