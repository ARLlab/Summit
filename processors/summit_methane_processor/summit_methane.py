"""
Methane injections are tricky. A 65-minute run contains 8 samples and 2 standards in the order:

sample x 2
std
sample x 4
std
sample x 2

PeakSimple records the start time of the run, which is ~3.5 minutes after the hour it begins on. From here,
the retention time of the peak is roughly 1 minute after the retention time of the peak. These number are subject to
change, but the rough formula will be:

sample_time = run_start_time + retention_time_in_minutes - 1_minute (sample concentration/equilibration before inject)

####################################
Class Structure:

This one's annoying...

GcRun - A whole run, contains 10 Samples
Sample - One injection of either ambient air, or standard - attached to one peak (this is almost a wrapper on Peak, but
	has lots of associated log info.)
Peak - A peak in the chromatogram, related one-one with a Sample

Process:


1) Create a GcRun, which is just a shell for the data along with some log parameters
	This requires the log information to come in as a dict, but no samples/peaks

2) Create Samples
	This requires the per-sample log information, which is then parsed to create the incomplete sample

3) Push Samples into GcRun
	Now, the log information has been processed completely

3) Create Peaks from CH4.LOG and match to Samples
	Peaks will be id'd with rough retention windows and given a sequence #, and take the largest within it above a limit
	None is okay, even likely given the GC
	These will be correctable later from the real integration sheet


"""

from pathlib import Path
import logging, json, os

from sqlalchemy.types import TypeDecorator, VARCHAR
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship

rundir = Path(r'C:\Users\brend\PycharmProjects\Summit\processors\summit_methane_processor')

Base = declarative_base()  # needed to subclass for sqlalchemy objects


class Peak(Base):
	"""
	A peak is just that, a signal peak in PeakSimple, Agilent, or another chromatography software.

	name: str, the compound name (if identified)
	mr: float, the mixing ratio (likely in ppbv) for the compound, if calculated; None if not
	pa: float, representing the area under the peak as integrated
	rt: float, retention time in minutes of the peak as integrated
	rev: int, represents the # of changes made to this peak's value
	qc: int, 0 = unreviewed, ...,  1 = final
	flag: int, ADD TODO ::
	int_notes, ADD TODO ::
	"""

	__tablename__ = 'peaks'

	id = Column(Integer, primary_key = True)
	name = Column(String)
	pa = Column(Float)
	mr = Column(Float)
	rt = Column(Float)
	rev = Column(Integer)
	qc = Column(Integer)
	gc_sequence_num = Column(Integer)

	run = relationship('GcRun', back_populates='peaks')
	run_id = ForeignKey(Integer, 'runs.id')

	sample = relationship('Sample', uselist=False, back_populates='samples')
	# TODO: Does it make more sense to have Peak, or Sample hold the foreignkey?

	def __init__(self, name, pa, rt):
		self.name = name.lower()
		self.pa = pa
		self.mr = None
		self.rt = rt
		self.rev = 0
		self.qc = 0

	def __str__(self):
		# Print the name, pa, and rt of a peak when called
		return f'<name: {self.name} pa: {self.pa} rt: {self.rt}, mr: {self.mr}>'

	def __repr__(self):
		# Print the name, pa, and rt of a peak when called
		return f'<name: {self.name} pa: {self.pa} rt: {self.rt}, mr: {self.mr}>'


class Sample(Base):
	__tablename__ = 'samples'

	id = Column(Integer, primary_key=True)

	peak = relationship('Peak', uselist=False, back_populates='sample')
	peak_id = ForeignKey(Integer, 'peaks.id')

	run = relationship('GcRun', back_populates='samples')
	run_id = ForeignKey(Integer, 'runs.id')




class GcRun(Base):
	"""
	A GcRun will contain all the peaks from that chromatogram, or line in the PeakSimple log. Usually, 8xsample, 2xstd.

	Peaks less than PA == 2, should be tossed (?)

	Standards within a run should have a stdev between the two, as well as some QC surrounding that.
	Samples should also have a run median and stdev that can be used for QC.

	If one standard in a run is poor, it should also re-quantify all samples from that run with the 'good' standard.
	"""

	__tablename__ = 'runs'

	id = Column(Integer, primary_key=True)
	peaks = relationship('Peak', back_populates='run')
	samples = relationship('Sample', back_populates='run')

	def __init__(self):
		pass


class Datum():
	"""
	A QC'd version of a peak...? Maybe, not sure yet.
	"""
	pass


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

	with TempDir(directory):
		engine = create_engine(engine_str)
	Session = sessionmaker(bind=engine)
	sess = Session()

	return engine, sess, Base


def read_log_file(path):
	"""
	A single log file actually accounts for a whole run, which includes 10 samples. There is information that is
	run-only, then sample-specific data. These need to be parsed at separate levels.

	:param path:
	:return:
	"""

	pass


def configure_logger(rundir):
	"""
	Create the project-specific logger. DEBUG and up is saved to the log, INFO and up appears in the console.

	:param rundir: path to create log sub-path in
	:return: logger object
	"""
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
	"""
	Returns the filesize in bytes.
	:param filepath: file-like object
	:return: int, filesize in bytes
	"""
	if Path.is_file(filepath):
		return Path.stat(filepath).st_size
	else:
		logger.warning(f'File {filepath.name} not found.')
		return


def list_files_recur(path):
	"""

	:param path: Path object
	:return: list, of file-like Path objects
	"""
	files = []
	for file in path.rglob('*'):
		files.append(file)

	return files


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


