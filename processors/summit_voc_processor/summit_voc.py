import logging
import datetime as dt
from datetime import datetime

from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from summit_core import JDict

Base = declarative_base()  # needed to subclass for sqlalchemy objects

log_params_list = (['filename', 'sampletime', 'sampleflow1', 'sampleflow2',
                    'sampletype', 'backflushtime', 'desorbtemp', 'flashheattime',
                    'injecttime', 'bakeouttemp', 'bakeouttime', 'carrierflow',
                    'samplenum', 'WTinuse', 'adsTinuse', 'samplepressure1',
                    'samplepressure2', 'GCHeadP', 'chamber_temp_start',
                    'WTA_temp_start', 'WTB_temp_start', 'adsA_temp_start',
                    'adsB_temp_start', 'samplecode', 'chamber_temp_end',
                    'WTA_temp_end', 'WTB_temp_end', 'adsA_temp_end',
                    'adsB_temp_end', 'traptempFH', 'GCstarttemp',
                    'traptempinject_end', 'traptempbakeout_end',
                    'WTA_hottemp', 'WTB_hottemp', 'GCHeadP1', 'GCoventemp'])
# does not include date on purpose because it's handled in GcRun

gcrun_params_list = log_params_list + ['peaks', 'date_end', 'date_start', 'crfs', 'type']

sample_types = {0: 'zero', 1: 'ba_standard', 2: 'bh_standard', 4: 'ambient', 6: 'trap_blank'}
# dict of all sample numbers and corresponding type names

compound_list = ['ethane', 'ethene', 'propane', 'propene', 'i-butane', '4b', 'acetylene',
                 'n-butane', '5a', '5b', '5c', 'i-pentane', 'cfc', 'n-pentane', 'hexane', 'benzene',
                 'toluene']

compound_ecns = ({'ethane': 2, 'ethene': 1.9, 'propane': 3, 'propene': 2.9,
                  'i-butane': 4, 'acetylene': 1.8, 'n-butane': 4, 'i-pentane': 5,
                  'n-pentane': 5, 'hexane': 6, 'benzene': 5.7, 'toluene': 6.7})
# expected carbon numbers for mixing ratio calcs

compound_windows = (
    {'ethane': (1.65, 1.85),  # compound retention windows for every named compound at Summit 'name':(low, high)
     'ethene': (2.04, 2.152),
     'propane': (2.85, 3.1),
     'propene': (5.75, 5.95),
     'i-butane': (7.3, 7.6),
     '4b': (0, 0),  # don't address 4b, acetylene, or n-butane for now
     'acetylene': (0, 0),
     'n-butane': (0, 0),
     '5a': (9.5, 10.3),
     '5b': (11.25, 11.65),
     '5c': (12.48, 12.55),
     'i-pentane': (12.60, 12.78),
     'cfc': (12.80, 13),
     'n-pentane': (13, 13.4),
     'hexane': (16.9, 17.05),
     'benzene': (19.95, 20.25),
     'toluene': (23.3, 23.75)})


class Crf(Base):
    """
    A crf is a set of carbon response factors for compounds, tied to a datetime and standard.
    These are assigned to GcRuns, which allows them to be integrated.

    Parameters:

    date_start : datetime, first datetime (inclusive) that the crf should be applied for
    date_end: datetime, last datetime that the crf is valid for (exclusive)
    date_revision: datetime, the time this CRF was added into the file
    standard: str, name of the standard this crf applies to
    compounds: dict, of compounds and the corresponding crf for each
    """

    __tablename__ = 'crfs'

    id = Column(Integer, primary_key=True)
    date_start = Column(DateTime, unique=True)
    date_end = Column(DateTime, unique=True)
    revision_date = Column(DateTime)
    standard = Column(String)
    compounds = Column(MutableDict.as_mutable(JDict))

    def __init__(self, date_start, date_end, date_revision, compounds, standard):
        self.date_start = date_start
        self.date_end = date_end
        self.date_revision = date_revision
        self.standard = standard
        self.compounds = compounds  # assign whole dict of CRFs

    def __str__(self):
        return f'<crf {self.standard} for {self.date_start} to {self.date_end}>'

    def __repr__(self):
        return f'<crf {self.standard} for {self.date_start} to {self.date_end}>'


class Peak(Base):
    """
    A peak is just that, a signal peak in PeakSimple, Agilent, or another
    chromatography software.
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

    id = Column(Integer, primary_key=True)
    name = Column(String)
    pa = Column(Float)
    mr = Column(Float)
    rt = Column(Float)
    rev = Column(Integer)
    qc = Column(Integer)

    line_id = Column(Integer, ForeignKey('nmhclines.id'))
    correction_id = Column(Integer, ForeignKey('nmhc_corrections.correction_id'))

    def __init__(self, name, pa, rt):
        self.name = name.lower()
        self.pa = pa
        self.mr = None
        self.rt = rt
        self.rev = 0
        self.qc = 0

    def get_name(self):
        return self.name

    def get_pa(self):
        return self.pa

    def get_rt(self):
        return self.rt

    def get_mr(self):
        return self.mr

    def set_name(self, name):
        self.name = name

    def set_pa(self, pa):
        self.pa = pa

    def set_rt(self, rt):
        self.rt = rt

    def set_mr(self, mr):
        self.mr = mr

    def __str__(self):
        # Print the name, pa, and rt of a peak when called
        return f'<name: {self.name} pa: {self.pa} rt: {self.rt}, mr: {self.mr}>'

    def __repr__(self):
        # Print the name, pa, and rt of a peak when called
        return f'<name: {self.name} pa: {self.pa} rt: {self.rt}, mr: {self.mr}>'


class NmhcLine(Base):
    """
    A line in NMHC_PA.LOG, which contains a datetime and some set of peaks.

    date: datetime, from the Python datetime library representing the time it was recorded by PeakSimple
    peaklist: list, a list of all the peak objects contained in the nmhc line.
    status: str, assigned as single to start, and when matched to a log will be 'married'
    """

    __tablename__ = 'nmhclines'

    id = Column(Integer, primary_key=True)
    date = Column(DateTime, unique=True)
    peaklist = relationship('Peak', order_by=Peak.id)
    status = Column(String)

    run_con = relationship('GcRun', uselist=False, back_populates='nmhc_con')
    nmhc_corr_con = relationship('NmhcCorrection', uselist=False, back_populates='nmhcline_con')
    nmhc_corr_id = Column(Integer, ForeignKey('nmhc_corrections.correction_id'))

    def __init__(self, date, peaks):
        self.date = date
        self.peaklist = peaks
        self.status = 'single'  # all logs begin unmatched

    def __str__(self):
        iso = self.date.isoformat(' ')
        return f'<NmhcLine for {iso}>'

    def __repr__(self):
        iso = self.date.isoformat(' ')
        return f'<NmhcLine for {iso}>'


class NmhcCorrection(NmhcLine):
    """
    A subclass of NmhcLine, this is linked to one NmhcLine, and is tied to a new table. All corrections are therefore
    also objects and recorded.

    status: str, either 'unapplied' or 'applied', rather than single/married as normal NmhcLines are
    """

    __tablename__ = 'nmhc_corrections'

    correction_id = Column(Integer, primary_key=True)
    peaklist = relationship('Peak', order_by=Peak.id)

    res_flag = Column(Integer)  # flag from the column in ambient_results_master.xlsx
    flag = Column(Integer)  # undetermined flagging system... TODO:

    nmhcline_con = relationship(NmhcLine, uselist=False, back_populates='nmhc_corr_con')
    correction_date = association_proxy('nmhcline_con', 'date')  # pass date from line to here

    # TODO:

    def __init__(self, nmhcline, peaks, res_flag, flag):
        # super().__init__(nmhcline.date, peaks) # nmhcline.date, peaks
        self.peaklist = peaks
        self.nmhcline_con = nmhcline
        self.status = 'unapplied'  # all corrections are created as unapplied
        self.res_flag = res_flag
        self.flag = flag

    def __str__(self):
        return f'<NmhcCorrection for {self.correction_date} with {len(self.peaklist)} peaks>'

    def __repr__(self):
        return f'<NmhcCorrection for {self.correction_date} with {len(self.peaklist)} peaks>'


class LogFile(Base):
    """
    The output from the LabView VI, it consists of all the listed parameters
    recorded in the file.

    filename: str, the name of the logfile in question
    date: datetime, the start of the 10-minute sampling period
    sampletime: float, the duration of the sample collection in seconds
    sampleflow1: float,
    sampleflow2: float,
    sampletype: int,
    desorbtemp: float,
    flashheattime: float,
    injecttime: float,
    bakeouttemp: float, temperature at bakeout
    bakeouttime: float,
    carrierflow: float,
    samplenum: int,
    WTinuse: int,
    adsTinuse: int,
    samplepressure1: float,
    samplepressure2: float
    GCHeadP: float,
    GCHeadP1: float,
    WTA_temp_start: float,
    WTB_temp_start: float,
    adsA_temp_start: float,
    adsB_temp_start: float,
    samplecode: int,
    chamber_temp_end:, float,
    WTA_temp_end: float,
    WTB_temp_end: float,
    adsA_temp_end: float,
    adsB_temp_end: float,
    traptempFH: float,
    GCstarttemp: float,
    traptempinject_end: float,
    traptempbakeout_end: float,
    WTA_hottemp: float,
    WTB_hottemp: float,
    GCoventemp: float,
    status: str, can be 'single' or 'married'
        A married status indicates the log has been matched to a pa line and is
        part of a GcRun object; single indicates unmatched/available for matching

    """

    __tablename__ = 'logfiles'

    id = Column(Integer, primary_key=True)
    filename = Column(String)
    date = Column(DateTime, unique=True)
    sampletime = Column(Float)
    sampleflow1 = Column(Float)
    sampleflow2 = Column(Float)
    sampletype = Column(Integer)
    backflushtime = Column(Float)
    desorbtemp = Column(Float)
    flashheattime = Column(Float)
    injecttime = Column(Float)
    bakeouttemp = Column(Float)
    bakeouttime = Column(Float)
    carrierflow = Column(Float)
    samplenum = Column(Integer)
    WTinuse = Column(Integer)
    adsTinuse = Column(Integer)
    samplepressure1 = Column(Float)
    samplepressure2 = Column(Float)
    GCHeadP = Column(Float)
    GCHeadP1 = Column(Float)
    chamber_temp_start = Column(Float)
    WTA_temp_start = Column(Float)
    WTB_temp_start = Column(Float)
    adsA_temp_start = Column(Float)
    adsB_temp_start = Column(Float)
    samplecode = Column(Integer)
    chamber_temp_end = Column(Float)
    WTA_temp_end = Column(Float)
    WTB_temp_end = Column(Float)
    adsA_temp_end = Column(Float)
    adsB_temp_end = Column(Float)
    traptempFH = Column(Float)
    GCstarttemp = Column(Float)
    traptempinject_end = Column(Float)
    traptempbakeout_end = Column(Float)
    GCoventemp = Column(Float)
    status = Column(String)

    run_con = relationship('GcRun', uselist=False, back_populates='log_con')

    def __init__(self, param_dict):
        # These are specified since a log_dict does not necessarily contain
        # every parameter, but all should be set, even if None
        self.filename = param_dict.get('filename', None)
        self.date = param_dict.get('date', None)
        self.sampletime = param_dict.get('sampletime', None)
        self.sampleflow1 = param_dict.get('sampleflow1', None)
        self.sampleflow2 = param_dict.get('sampleflow2', None)
        self.sampletype = param_dict.get('sampletype', None)
        self.backflushtime = param_dict.get('backflushtime', None)
        self.desorbtemp = param_dict.get('desorbtemp', None)
        self.flashheattime = param_dict.get('flashheattime', None)
        self.injecttime = param_dict.get('injecttime', None)
        self.bakeouttemp = param_dict.get('bakeouttemp', None)
        self.bakeouttime = param_dict.get('bakeouttime', None)
        self.carrierflow = param_dict.get('carrierflow', None)
        self.samplenum = param_dict.get('samplenum', None)
        self.WTinuse = param_dict.get('WTinuse', None)
        self.adsTinuse = param_dict.get('adsTinuse', None)
        self.samplepressure1 = param_dict.get('samplepressure1', None)
        self.samplepressure2 = param_dict.get('samplepressure2', None)
        self.GCHeadP = param_dict.get('GCHeadP', None)
        self.GCHeadP1 = param_dict.get('GCHeadP1', None)
        self.chamber_temp_start = param_dict.get('chamber_temp_start', None)
        self.WTA_temp_start = param_dict.get('WTA_temp_start', None)
        self.WTB_temp_start = param_dict.get('WTB_temp_start', None)
        self.adsA_temp_start = param_dict.get('adsA_temp_start', None)
        self.adsB_temp_start = param_dict.get('adsB_temp_start', None)
        self.samplecode = param_dict.get('samplecode', None)
        self.chamber_temp_end = param_dict.get('chamber_temp_end', None)
        self.WTA_temp_end = param_dict.get('WTA_temp_end', None)
        self.WTB_temp_end = param_dict.get('WTB_temp_end', None)
        self.adsA_temp_end = param_dict.get('adsA_temp_end', None)
        self.adsB_temp_end = param_dict.get('adsB_temp_end', None)
        self.traptempFH = param_dict.get('traptempFH', None)
        self.GCstarttemp = param_dict.get('GCstarttemp', None)
        self.traptempinject_end = param_dict.get('traptempinject_end', None)
        self.traptempbakeout_end = param_dict.get('traptempbakeout_end', None)
        self.WTA_hottemp = param_dict.get('WTA_hottemp', None)
        self.WTB_hottemp = param_dict.get('WTB_hottemp', None)
        self.GCoventemp = param_dict.get('GCoventemp', None)
        self.status = 'single'

    def __str__(self):
        # Print the log's status, filename, and ISO datetime
        iso = self.date.isoformat(' ')
        return f'<{self.status} log {self.filename} at {iso}>'

    def __repr__(self):
        # Print the log's status, filename, and ISO datetime
        iso = self.date.isoformat(' ')
        return f'<{self.status} log {self.filename} at {iso}>'


class GcRun(Base):
    """
    A run, which consists of the attributes taken from the NmhcLine and LogFile
    that are used to create it. This is now a confirmed run, meaning it was executed
    in PeakSimple and the VI.

    peaks: list; of Peaks, the peaks associated with the NmhcLine associated with this file
    date_end: datetime, the date that the sample was recorded by PeakSimple
        Roughly representative of the end of the 10-minute sampling window
    date_start: datetime, the time the run-log was recorded by LabView
        Roughly representative of the start of the 10-minute sampling window
    log_params_list: See LogFile class for list (those added here do not include [date, status])

    crf: object, contains a dict of compound response factors for calculating a mixing ratio

    type: str, converted internally with a dict to give the sampletype a str-name
        {0:'blank',1:'',2:'',...7:''}

    When integrated, all the peak objects of a GcRun will gain a self.mr. These
    are kept as part of a GcRun, but references to these should be under datum.
    """

    __tablename__ = 'gcruns'

    id = Column(Integer, primary_key=True)
    type = Column(String)
    date = Column(DateTime)

    nmhcline_id = Column(Integer, ForeignKey('nmhclines.id'))
    nmhc_con = relationship('NmhcLine', uselist=False, foreign_keys=[nmhcline_id], back_populates='run_con')
    peaks = association_proxy('nmhc_con', 'peaklist')
    date_end = association_proxy('nmhc_con', 'date')  # pass date from NMHC to GcRun

    logfile_id = Column(Integer, ForeignKey('logfiles.id'))
    log_con = relationship('LogFile', uselist=False, foreign_keys=[logfile_id], back_populates='run_con')
    date_start = association_proxy('log_con', 'date')  # pass date from LogFile to GcRun

    for attr in log_params_list:
        vars()[attr] = association_proxy('log_con', attr)  # set association_proxy(s)
    # for all log parameters to pass them to this GC instance

    data_id = Column(Integer, ForeignKey('data.id'))
    data_con = relationship('Datum', uselist=False, foreign_keys=[data_id], back_populates='run_con')

    crfs = relationship('Crf', uselist=False)
    crf_id = Column(Integer, ForeignKey('crfs.id'))

    def __init__(self, LogFile, NmhcLine):
        self.nmhc_con = NmhcLine
        self.log_con = LogFile
        self.data_con = None
        self.crfs = None  # begins with no crf, will be found later
        self.type = sample_types.get(self.sampletype, None)

        self.date = self.log_con.date + (self.nmhc_con.date - self.log_con.date) / 2

    # date of GcRun is the average of the log date and paline date

    def __str__(self):
        return f'<matched gc run at {self.date_end}>'

    def _repr__(self):
        return f'<matched gc run at {self.date_end}>'

    ## GET Methods for all embedded objects of a datum

    def get_mr(self, compound_name):
        return next((peak.mr for peak in self.peaks if peak.name == compound_name), None)

    def get_pa(self, compound_name):
        return next((peak.pa for peak in self.peaks if peak.name == compound_name), None)

    def get_rt(self, compound_name):
        return next((peak.rt for peak in self.peaks if peak.name == compound_name), None)

    def get_unnamed_peaks(self):
        # returns list of unidentified peaks in a run
        return [peak for peak in self.peaks if peak.name == '-']

    def get_crf(self, compound_name):
        # returns the crf for the given compound as a float
        return self.crfs.compounds.get(compound_name, None)

    def integrate(self):
        if self.crfs is None:
            return None  # no crfs, no integration!
        elif self.type == 'ambient' or self.type == 'zero':
            for peak in self.peaks:
                if peak.name in compound_list and peak.name in self.crfs.compounds.keys():
                    crf = self.crfs.compounds[peak.name]

                    peak.mr = ((peak.pa /
                                (crf * compound_ecns.get(peak.name, None) * self.sampletime * self.sampleflow1))
                               * 1000 * 1.5)

            # formula is (pa / (CRF * ECN * SampleTime * SampleFlow1)) * 1000 *1.5
            # 1000 * 1.5 normalizes to a sample volume of 2000s by convention

            return Datum(self)
        else:
            return None  # don't integrate if it's not an ambient or blank sample


class Datum(Base):
    """
    A point of the plural data. This is a gc run that has been integrated, has a
    mixing ratio (which can be None if it is a failed or QC removed run -- that we
    took a measurement is valuable information to report). It has a flag, revision,
    qc status, and .... TODO <<<

    mr: float, the mixing ratio of the gas
    unit: dict, ?
    flag: object, ... TODO <<< num flags for separate data submission types, like a built-in for EBAS?
    sig_fig: dict, ?
    standard_used: object, ... TODO <<<
    revision:
    qc: str,
    notes: str, any notes about the data quality, processing, or other
    """

    __tablename__ = 'data'

    id = Column(Integer, primary_key=True)
    revision = Column(Integer)
    qc = Column(Integer)
    notes = Column(String)

    run_con = relationship('GcRun', uselist=False, back_populates='data_con')

    for attr in gcrun_params_list:
        vars()[attr] = association_proxy('run_con', attr)  # set association_proxy(s)

    # for all log parameters to pass them to this GC instance

    def __init__(self, GcRun):
        self.run_con = GcRun
        self.revision = 0  # init with revision status zero
        self.qc = 0
        self.notes = None

    def __str__(self):
        return f'<data for {self.date_end} with {len(self.peaks)} peaks>'

    def __repr__(self):
        return f'<data for {self.date_end} with {len(self.peaks)} peaks>'

    ## GET Methods for all embedded objects of a datum
    # These are resource-expensive, but can be used for one-offs where queries are unnecesary or tedious

    def get_mr(self, compound_name):
        return next((peak.mr for peak in self.peaks if peak.name == compound_name), None)

    def get_pa(self, compound_name):
        return next((peak.pa for peak in self.peaks if peak.name == compound_name), None)

    def get_rt(self, compound_name):
        return next((peak.rt for peak in self.peaks if peak.name == compound_name), None)

    def get_crf(self, compound_name):
        return self.crfs.compounds.get(compound_name, None)


def find_crf(crfs, sample_date):
    """
    Returns the carbon response factor object for a sample at the given sample_date
    :param crfs: list, of Crf objects
    :param sample_date: datetime, datetime of sample to be matched to a CRF
    :return: Crf object
    """

    return next((crf for crf in crfs if crf.date_start <= sample_date < crf.date_end), None)


def read_crf_data(filename):
    """
    Read the CRF information from the given filename.
    :param filename: string, name of file
    :return: list, of Crf objects
    """

    lines = open(filename).readlines()

    compounds = dict()

    keys = lines[0].split('\t')[3:]  # list of strs of all compound names from file

    Crfs = []

    for line in lines[1:]:
        ls = line.split('\t')
        date_start = datetime.strptime(ls[0], '%m/%d/%Y %H:%M')
        date_end = datetime.strptime(ls[1], '%m/%d/%Y %H:%M')
        date_revision = datetime.strptime(ls[2], '%m/%d/%Y %H:%M')

        for index, (key, rf) in enumerate(zip(keys, ls[3:])):
            key = key.strip().lower()
            # unpack all names from header as keys and line items as values
            compounds[key] = float(rf)

        if date_start is not None and date_end is not None:
            Crfs.append(Crf(date_start, date_end, date_revision, compounds, 'working standard'))

    return Crfs


def read_log_file(filename):
    """
    Processes Summit LabView files into a dictionary that's unpacked into a LogFile object
    :param filename: string, filename to be read
    :return: LogFile, or None
    """
    logger = logging.getLogger(__name__)

    with open(filename) as file:
        contents = file.readlines()

        log_dict = dict()

        try:
            if len(contents) == 36:
                log_dict['filename'] = file.name
                log_dict['date'] = datetime.strptime(contents[15].split('\t')[0], '%Y%j%H%M%S')
                log_dict['sampletime'] = float(contents[0].split('\t')[1])
                log_dict['sampleflow1'] = float(contents[1].split('\t')[1])
                log_dict['sampleflow2'] = float(contents[22].split('\t')[1])
                log_dict['sampletype'] = int(float(contents[2].split('\t')[1]))
                log_dict['backflushtime'] = float(contents[3].split('\t')[1])
                log_dict['desorbtemp'] = float(contents[4].split('\t')[1])
                log_dict['flashheattime'] = float(contents[5].split('\t')[1])
                log_dict['injecttime'] = float(contents[6].split('\t')[1])
                log_dict['bakeouttemp'] = float(contents[7].split('\t')[1])
                log_dict['bakeouttime'] = float(contents[8].split('\t')[1])
                log_dict['carrierflow'] = float(contents[9].split('\t')[1])
                log_dict['samplenum'] = int(float(contents[10].split('\t')[1]))
                log_dict['WTinuse'] = int(float(contents[11].split('\t')[1]))
                log_dict['adsTinuse'] = int(float(contents[12].split('\t')[1]))
                log_dict['samplepressure1'] = float(contents[13].split('\t')[1])
                log_dict['samplepressure2'] = float(contents[21].split('\t')[1])
                log_dict['GCHeadP'] = float(contents[14].split('\t')[1])
                log_dict['chamber_temp_start'] = float(contents[16].split('\t')[1])
                log_dict['WTA_temp_start'] = float(contents[17].split('\t')[1])
                log_dict['WTB_temp_start'] = float(contents[18].split('\t')[1])
                log_dict['adsA_temp_start'] = float(contents[19].split('\t')[1])
                log_dict['adsB_temp_start'] = float(contents[20].split('\t')[1])
                log_dict['samplecode'] = int(float(contents[15].split('\t')[0]))
                log_dict['chamber_temp_end'] = float(contents[23].split('\t')[1])
                log_dict['WTA_temp_end'] = float(contents[24].split('\t')[1])
                log_dict['WTB_temp_end'] = float(contents[25].split('\t')[1])
                log_dict['adsA_temp_end'] = float(contents[26].split('\t')[1])
                log_dict['adsB_temp_end'] = float(contents[27].split('\t')[1])
                log_dict['traptempFH'] = float(contents[28].split('\t')[1])
                log_dict['GCstarttemp'] = float(contents[29].split('\t')[1])
                log_dict['traptempinject_end'] = float(contents[30].split('\t')[1])
                log_dict['traptempbakeout_end'] = float(contents[31].split('\t')[1])
                log_dict['WTA_hottemp'] = float(contents[32].split('\t')[1])
                log_dict['WTB_hottemp'] = float(contents[33].split('\t')[1])
                log_dict['GCHeadP1'] = float(contents[34].split('\t')[1])
                log_dict['GCoventemp'] = float(contents[35].split('\t')[1])

                return LogFile(log_dict)

            else:
                logger.warning(f'File {file.name} had an improper number of lines and was ignored.')
                # print(f'File {file.name} had an improper number of lines and was ignored.')
                return None
        except:
            logger.warning(f'File {file.name} failed to be processed and was ignored.')
            # print(f'File {file.name} failed to be processed and was ignored.')
            return None


def read_pa_line(line):
    """
    Takes one line as a string from a PeakSimple log, and processes it in Peak objects and an NmhcLine containing those
    peaks.
    :param line: string, one line of data from VOC.LOG, NMHC_PA.LOG, etc.
    :return: NmhcLine or None
    """

    ls = line.split('\t')
    line_peaks = []

    line_date = datetime.strptime(ls[1] + ' ' + ls[2], '%m/%d/%Y %H:%M:%S')

    for ind, item in enumerate(ls[3:]):

        ind = ind + 3  # offset ind since we're working with ls[3:]

        peak_dict = dict()

        if '"' in item:

            peak_dict['name'] = item.strip('"')  # can't fail, " is definitely there

            try:
                peak_dict['rt'] = float(ls[ind + 1])
            except:
                peak_dict['rt'] = None
            try:
                peak_dict['pa'] = float(ls[ind + 2])
            except:
                peak_dict['pa'] = None

            if None not in peak_dict.values():
                line_peaks.append(Peak(peak_dict['name'], peak_dict['pa'], peak_dict['rt']))

    if len(line_peaks) == 0:
        this_line = None
    else:
        this_line = NmhcLine(line_date, line_peaks)

    return this_line


# def find_closest_date(date, list_of_dates):
# 	"""
# 	This is a helper function that works on Python datetimes. It returns the closest date value,
# 	and the timedelta from the provided date.
# 	:param date: datetime
# 	:param list_of_dates: list, of datetimes
# 	:return: match, delta: the matching date from the list, and it's difference to the original as a timedelta
# 	"""
# 	match = min(list_of_dates, key = lambda x: abs(x - date))
# 	delta = match - date
#
# 	return match, delta
#
#
# def search_for_attr_value(obj_list, attr, value):
# 	"""
# 	Finds the first (not necesarilly the only) object in a list, where its
# 	attribute 'attr' is equal to 'value', returns None if none is found.
# 	:param obj_list: list, of objects to search
# 	:param attr: string, attribute to search for
# 	:param value: mixed types, value that should be searched for
# 	:return: obj, from obj_list, where attribute attr matches value
# 		**** warning: returns the *first* obj, not necessarily the only
# 	"""
# 	return next((obj for obj in obj_list if getattr(obj,attr, None) == value), None)


def match_log_to_pa(LogFiles, NmhcLines):
    """
    This takes a list of LogFile and NmhcLine objects and returns a list (empty, even)
        of resulting GcRun objects. When matching objects, it WILL modify their parameters
        and status if warranted.
    :param LogFiles: list, of LogFile objects that are unmatched
    :param NmhcLines: list, of NmhcLine objects that are unmatched
    :return: list, of GcRun objects created by matched LogFile/NmhcLine pairs
    """

    from summit_core import find_closest_date, search_for_attr_value

    runs = []
    for log in LogFiles:
        # For each log, attempt to find matching NmhcLine
        # unpack date attr from all NmhcLines provided
        nmhc_dates = [line.date for line in NmhcLines]

        [match, diff] = find_closest_date(log.date, nmhc_dates)  # get matching date and it's difference

        if abs(diff) < dt.timedelta(minutes=40):
            # Valid matches *usually* have ~35min diffs, but shorter means complications may have occurred.
            # They should still match if shorter, though.

            matched_line = search_for_attr_value(NmhcLines, 'date', match)  # pull matching NmhcLine

            runs.append(GcRun(log, matched_line))
            log.status = 'married'
            matched_line.status = 'married'
        else:
            continue

    return runs


def get_dates_peak_info(session, compound, info, date_start=None, date_end=None):
    """

    :param session: An active sqlalchemy session object
    :param compound: string, the compound to be retrieved
    :param info: string, the item from ['mr','pa', 'rt'] to be retrieved
    :param date_start: datetime, start of the period to be retrieved
    :param date_end: datetime, end of the period to be retrieved
    :return: info, dates; lists of the requested info and corresponding dates
    """

    peak_info = getattr(Peak, info, None)

    if peak_info is None:
        return None, None

    if date_start is None and date_end is None:
        try:
            peak_info = (session.query(peak_info, LogFile.date).filter(Peak.name == compound, GcRun.type == 'ambient')
                         .join(NmhcLine).join(GcRun).join(LogFile).order_by(LogFile.date))  # get everything
            info, dates = zip(*peak_info.all())
        except ValueError:
            info = None
            dates = None

        return info, dates

    elif date_start is None:
        try:
            peak_info = (session.query(peak_info, LogFile.date).filter(Peak.name == compound, GcRun.type == 'ambient')
                         .join(NmhcLine).join(GcRun).join(LogFile)
                         .filter(LogFile.date < date_end).order_by(LogFile.date))  # get only before the end date given

            info, dates = zip(*peak_info.all())
        except ValueError:
            info = None
            dates = None

        return info, dates

    elif date_end is None:
        try:
            peak_info = (session.query(peak_info, LogFile.date)
                         .filter(Peak.name == compound, GcRun.type == 'ambient')
                         .join(NmhcLine).join(GcRun).join(LogFile)
                         .filter(LogFile.date > date_start).order_by(LogFile.date))
            info, dates = zip(*peak_info.all())  # get only after the start date given
        except ValueError:
            info = None
            dates = None

        return info, dates

    else:
        try:
            peak_info = (session.query(peak_info, LogFile.date).filter(Peak.name == compound, GcRun.type == 'ambient')
                .join(NmhcLine).join(GcRun).join(LogFile)
                .filter(LogFile.date.between(date_start, date_end)).order_by(
                LogFile.date))  # get between date bookends (inclusive beginning!)
            info, dates = zip(*peak_info.all())
        except ValueError:
            info = None
            dates = None

        return info, dates


def summit_voc_plot(dates, compound_dict, limits=None, minor_ticks=None, major_ticks=None):
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
        compounds_safe.append(k.replace('-', '_').replace('/', '_').lower())

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

    ax.set_ylabel('Mixing Ratio (ppbv)', fontsize=20)
    ax.set_title(f'{comp_list}', fontsize=24, y=1.02)
    ax.legend(compound_dict.keys())

    f1.subplots_adjust(bottom=.20)

    f1.savefig(f'{fn_list}_last_week.png', dpi=150)
    plt.close(f1)


def get_peak_data(run):
    """
    Useful for extracing all peak info from newly created GcRuns or Datums in service of integration corrections.
    :param run: GcRun object
    :return: list, list; the peak areas and retention times for all components of the run
    """

    pas = [peak.pa for peak in run.peaks]
    rts = [peak.rt for peak in run.peaks]

    assert len(pas) == len(rts), "get_peak_data() produced lists of uneven lengths."

    return (pas, rts)


def name_summit_peaks(nmhcline):
    """
    'Simple' peak identification based on retention times. The C4 compounds get more rigorous treatment, though.
    :param nmhcline: NmhcLine object
    :return: NmhcLine object, with named peaks
    """

    from summit_core import search_for_attr_value

    compound_pools = dict()

    for peak in nmhcline.peaklist:
        for compound, limits in compound_windows.items():
            if limits[0] < peak.get_rt() < limits[1]:
                try:  # add peak to pool for that compound and stop attempting to assign it. NEXT!
                    compound_pools[compound].append(peak)
                    break
                except KeyError:
                    compound_pools[compound] = [peak]
                    break

    for name, pool in compound_pools.items():
        """Address all EXCEPT n-butane, acetylene, and 4b in this loop."""
        if name not in ['n-butane', 'acetylene', '4b']:

            if len(pool) is 1:
                chosen = pool[0]  # take only value if there
            else:
                chosen = max(pool, key=lambda peak: peak.pa)  # get largest peak in possible peaks

            if chosen is not None:
                chosen.set_name(name)

    ibut = search_for_attr_value(nmhcline.peaklist, 'name', 'i-butane')

    find_nbut = True
    find_acet = True  # default to both needing to be found

    if ibut is not None:
        ibut_rt = ibut.get_rt()

        if find_acet:
            acet_pool = [peak for peak in nmhcline.peaklist if .6 < (peak.rt - ibut_rt) < .7]
            # print('acet', acet_pool)
            if len(acet_pool) == 0:
                pass
            else:
                acet = max(acet_pool, key=lambda peak: peak.pa)  # get largest peak in possible peaks
                acet.name = 'acetylene'

        if find_nbut:
            nbut_pool = [peak for peak in nmhcline.peaklist if .55 < (peak.rt - ibut_rt) < .60]
            # print('nbut', nbut_pool)
            if len(nbut_pool) == 0:
                pass
            else:
                nbut = max(nbut_pool, key=lambda peak: peak.pa)  # get largest peak in possible peaks
                nbut.name = 'n-butane'

    return nmhcline
