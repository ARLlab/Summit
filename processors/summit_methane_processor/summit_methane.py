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

2) Push peaks into the GcRun

3) Create Samples
	This requires the per-sample log information, which is then parsed to create the incomplete sample

	Peaks will be id'd with rough retention windows and given a sequence #, and take the largest within it above a limit
	None is okay, even likely given the GC
	These will be correctable later from the real integration sheet

4) Create Samples in GcRun method, pass rt window dict to find correct peaks
	Now, the log information has been processed completely

	##############################
	NEW

	Peaks get a date
	All peaks get committed from new lines in the pa file (distinguish by date)
	Then peaks gets queried per GcRun for within a date range

"""

from pathlib import Path
import logging, json, os
from datetime import datetime
import datetime as dt
import statistics as s

from sqlalchemy.types import TypeDecorator, VARCHAR
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship

rundir = Path(r'C:\Users\brend\PycharmProjects\Summit\processors\summit_methane_processor')
# rundir = Path(r'C:\Users\arl\Desktop\summit_master\processors\summit_methane_processor')

Base = declarative_base()  # needed to subclass for sqlalchemy objects

# retention times based on sample number
sample_rts = {0: [2, 3],
			  1: [8.3, 9.3],
			  2: [14.65, 15.65],
			  3: [21, 22],
			  4: [27.3, 28.3],
			  5: [33.65, 34.65],
			  6: [40, 41],
			  7: [46.4, 47.4],
			  8: [52.75, 53.75],
			  9: [59, 60]}


class Standard(Base):

	__tablename__ = 'standards'

	id = Column(Integer, primary_key=True)
	name = Column(String)
	mr = Column(Float)
	date_st = Column(DateTime)
	date_en = Column(DateTime)

	sample = relationship('Sample', back_populates='standard')

	def __init__(self, name, mr, date_st, date_en):
		self.name = name
		self.mr = mr
		self.date_st = date_st
		self.date_en = date_en


class Peak(Base):
	"""
	A peak is just that, a signal peak in PeakSimple, Agilent, or another chromatography software.

	name: str, the compound name (if identified)
	mr: float, the mixing ratio (likely in ppbv) for the compound, if calculated; None if not
	pa: float, representing the area under the peak as integrated
	rt: float, retention time in minutes of the peak as integrated
	rev: int, represents the # of changes made to this peak's value
	qc: int, 0 = unreviewed, ...,  1 = final
	flag: int, ADD
	int_notes, ADD
	"""

	__tablename__ = 'peaks'

	id = Column(Integer, primary_key = True)
	name = Column(String)
	pa = Column(Float)
	mr = Column(Float)
	rt = Column(Float)
	rev = Column(Integer)
	qc = Column(Integer)

	run = relationship('GcRun', back_populates='peaks')
	run_id = Column(Integer, ForeignKey('runs.id'))

	pa_line = relationship('PaLine', back_populates='peaks')
	pa_line_id = Column(Integer, ForeignKey('palines.id'))

	sample = relationship('Sample', uselist=False, back_populates='peak')

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
	date = Column(DateTime)

	peak = relationship('Peak', uselist=False, back_populates='sample')
	peak_id = Column(Integer, ForeignKey('peaks.id'))

	run = relationship('GcRun', back_populates='samples')
	run_id = Column(Integer, ForeignKey('runs.id'))

	standard = relationship('Standard', back_populates='sample')
	standard_id = Column(Integer, ForeignKey('standards.id'))

	quantifier = relationship('Sample', remote_side=[id])  # relate a sample to it's quantifying sample if applicable
	quantifier_id = Column(Integer, ForeignKey('samples.id'))

	flow = Column(Float)
	pressure = Column(Float)
	rh = Column(Float)
	relax_p = Column(Float)
	sample_type = Column(Integer)
	sample_num = Column(Integer)

	def __init__(self, run, flow, pressure, rh, relax_p, sample_type, sample_num):
		self.run = run
		self.flow = flow
		self.pressure = pressure
		self.rh = rh
		self.relax_p = relax_p
		self.sample_type = sample_type
		self.sample_num = sample_num


class PaLine(Base):

	__tablename__ = 'palines'

	id = Column(Integer, primary_key=True)
	peaks = relationship('Peak', back_populates='pa_line')
	run = relationship('GcRun', back_populates='pa_line')
	date = Column(DateTime)
	status = Column(String)

	def __init__(self, date, peaks):
		self.date = date
		self.peaks = peaks
		self.status = 'single'


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
	pa_line = relationship('PaLine', back_populates='run')
	pa_line_id = Column(Integer, ForeignKey('palines.id'))
	median = Column(Float)
	rsd = Column(Float)  # relative standard deviation (stdev/median) * 100
	standard_rsd = Column(Float)

	_logfile = Column(String)
	date = Column(DateTime)
	carrier_flow = Column(Float)
	sample_flow = Column(Float)
	sample_time = Column(Float)
	relax_time = Column(Float)
	injection_time = Column(Float)
	wait_time = Column(Float)
	loop_p_check1 = Column(Float)
	loop_p_check2 = Column(Float)
	status = Column(String)

	def __init__(self, logfile, date, carrier_flow, sample_flow, sample_time, relax_time,
				 injection_time, wait_time, loop_p_check1, loop_p_check2):
		self.carrier_flow = carrier_flow
		self.sample_flow = sample_flow
		self.sample_time = sample_time
		self.relax_time = relax_time
		self.injection_time = injection_time
		self.wait_time = wait_time
		self.loop_p_check1 = loop_p_check1
		self.loop_p_check2 = loop_p_check2
		self.logfile = logfile
		self.date = date
		self.status = 'single'

	@property  # lets logfile be stored as String, but returns Path object
	def logfile(self):
		return Path(self._logfile)

	@logfile.setter
	def logfile(self, value):
		self._logfile = str(value)


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


def read_pa_line(line):
	"""
	Takes one line as a string from a PeakSimple log, and processes it in Peak objects and an NmhcLine containing those
	peaks. (This one is a minor modification of the one in summit_voc.py)
	:param line: string, one line of data from VOC.LOG, NMHC_PA.LOG, etc.
	:return: NmhcLine or None
	"""

	ls = line.split('\t')
	line_peaks = []

	line_date = datetime.strptime(ls[1] + ' ' + ls[2], '%m/%d/%Y %H:%M:%S')

	for ind, item in enumerate(ls[3:]):

		ind = ind+3 # offset ind since we're working with ls[3:]

		peak_dict = dict()

		if '"' in item:

			peak_dict['name'] = item.strip('"') # can't fail, " is definitely there

			try:
				peak_dict['rt'] = float(ls[ind+1])
			except:
				peak_dict['rt'] = None
			try:
				peak_dict['pa'] = float(ls[ind+2])
			except:
				peak_dict['pa'] = None

			if None not in peak_dict.values():
				line_peaks.append(Peak(peak_dict['name'], peak_dict['pa'], peak_dict['rt']))

	if len(line_peaks) == 0:
		this_line = None
	else:
		this_line = PaLine(line_date, line_peaks)

	return this_line


def read_log_file(path):
	"""
	A single log file actually accounts for a whole run, which includes 10 samples. There is information that is
	run-only, then sample-specific data. These need to be parsed at separate levels.

	:param path:
	:return:
	"""

	contents = path.read_text().split('\n')

	# filter out blank line the VI is writing at the end of files that read_text() keeps
	contents[:] = [line for line in contents if line != '']

	run_year = int(path.name[:4])
	run_doy = int(path.name[4:7]) - 1  # when adding, if it's DOY 1, you don't want to add 1 to Jan 1 of that year...
	run_hour = int(path.name[7:9])

	run_date = datetime(run_year, 1, 1) + dt.timedelta(days=run_doy, hours=run_hour)

	run_dict = {}
	run_dict['logfile'] = path
	run_dict['date'] = run_date
	run_dict['carrier_flow'] = float(contents[0].split('\t')[1])
	run_dict['sample_flow'] = float(contents[1].split('\t')[1])
	run_dict['sample_time'] = float(contents[2].split('\t')[1])
	run_dict['relax_time'] = float(contents[3].split('\t')[1])
	run_dict['injection_time'] = float(contents[4].split('\t')[1])
	run_dict['wait_time'] = float(contents[5].split('\t')[1])
	run_dict['loop_p_check1'] = float(contents[-2].split('\t')[1])
	run_dict['loop_p_check2'] = float(contents[-1].split('\t')[1])

	run = GcRun(**run_dict)

	# run information is contained in 23-line blocks with no delimiters, spanning (0-indexed) lines 17:-3
	# each block of 23 will
	run_blocks = contents[17:-2]
	# run blocks come in sets of 23 lines with no particular delimiters
	indices = [i*23 for i in range(int(len(run_blocks)/23))]

	samples = []
	for num, ind in enumerate(indices):
		sample_info = run_blocks[ind:ind+23]
		sample_dict = {}
		sample_dict['flow'] = float(sample_info[0].split('\t')[1])
		sample_dict['pressure'] = float(sample_info[1].split('\t')[1])
		sample_dict['rh'] = float(sample_info[2].split('\t')[1])
		sample_dict['relax_p'] = s.mean([float(sample_info[i].split('\t')[1]) for i in range(3, 23)])
		sample_dict['sample_type'] = int(float(contents[6+num].split('\t')[1]))  # get sample type (int) from file
		sample_dict['sample_num'] = num  # assign number of sample in the GC sequence

		samples.append(Sample(run, **sample_dict))

	run.samples = samples

	return run


def configure_logger(rundir):
	"""
	Create the project-specific logger. DEBUG and up is saved to the log, INFO and up appears in the console.

	:param rundir: path to create log sub-path in
	:return: logger object
	"""
	logfile = Path(rundir) / 'processor_logs/summit_methane.log'
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


def get_all_data_files(path):
	"""
	Recursively search the given directory for .xxx files.

	:param rundir_path:
	:return: list, of file-like Path objects
	"""
	files = list_files_recur(path)
	files[:] = [file for file in files if '.txt' in file.name]

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


def find_closest_date(date, list_of_dates):
	"""
	This is a helper function that works on Python datetimes. It returns the closest date value,
	and the timedelta from the provided date.
	:param date: datetime
	:param list_of_dates: list, of datetimes
	:return: match, delta: the matching date from the list, and it's difference to the original as a timedelta
	"""
	match = min(list_of_dates, key = lambda x: abs(x - date))
	delta = match - date

	return match, delta


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
	return next((obj for obj in obj_list if getattr(obj,attr, None) == value), None)


def match_lines_to_runs(lines, runs):
	"""
	This takes a list of PaLine and GcRun objects and matched them by date, within a tolerance.
	When matching objects, it WILL modify their parameters and status if warranted.

	:param lines: list, of PaLine objects that are unmatched
	:param runs: list, of GcRun objects that are unmatched
	:return: (lines, runs, match_count) list of line objects, list of run objects, and int of runs that were matched
	"""
	match_count = 0

	for line in lines:
		# For each log, attempt to find matching NmhcLine
		# unpack date attr from all NmhcLines provided
		run_dates = [run.date for run in runs]

		[match, diff] = find_closest_date(line.date, run_dates)  # get matching date and it's difference

		if abs(diff) < dt.timedelta(minutes=5):
			# Valid matches *usually* have ~03:22 difference

			matched_run = search_for_attr_value(runs, 'date', match)  # pull matching NmhcLine

			line.status = 'married'
			matched_run.status = 'married'

			for peak in line.peaks:
				peak.run = matched_run  # relate all peaks in pa line to the newly matched run

			matched_run.pa_line = line
			logger.info(f'PaLine {line.date} matched to GcRun for {matched_run.date}.')
			match_count += 1
		else:
			continue

	return (lines, runs, match_count)


def calc_sample_date(run, sample, peak):
	"""
	This takes the matched run, sample, and peak combination and creates an estimate for the sample collection of this
	individual sample based on the retention time of the peak, start of the chromatogram, and expected injeciton time.

	sample_time = run_start_time + retention_time_in_minutes - 1_minute (sample concentration/equilibration before inject)

	:param run:
	:param sample:
	:param peak:
	:return:
	"""



	return run, sample, peak


def calc_ch4_mr(sample, quantifier, standard):
	sample.quantifier = quantifier
	sample.peak.mr = (sample.peak.pa / sample.quantifier.peak.pa) * standard.mr
	sample.standard = standard

	return sample


def valid_sample(sample):
	if sample is None or sample.peak is None or sample.peak.pa is None:
		return False
	else:
		return True
