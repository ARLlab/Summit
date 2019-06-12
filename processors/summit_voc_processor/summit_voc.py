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

log_parameter_bounds = ({'samplepressure1': (1.5, 2.65),
                         'samplepressure2': (6.5, 10),
                         'GCHeadP': (5, 7.75),
                         'GCHeadP1': (9, 13),
                         'chamber_temp_start': (18, 30),
                         'WT_primary_temp_start': (-35, -24),
                         'WT_secondary_temp_start': (18, 35),
                         'ads_secondary_temp_start': (18, 35),
                         'ads_primary_temp_start': (-35, -24),
                         'chamber_temp_end': (18, 30),
                         'WT_primary_temp_end': (-35, -24),
                         'WT_secondary_temp_end': (15, 35),
                         'ads_secondary_temp_end': (15, 35),
                         'ads_primary_temp_end': (-35, -24),
                         'traptempFH': (-35, 0),
                         'GCstarttemp': (35, 45),
                         'traptempinject_end': (285, 310),
                         'traptempbakeout_end': (310, 335),
                         'WT_primary_hottemp': (75, 85),
                         'WT_secondary_hottemp': (20, 35),
                         'GCoventemp': (190, 210)})  # acceptable limits for parameters that are checked

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

sheet_slices = {
    'ambient': {'start': 43, 'end': 59, 'diff': 54},
    'ba': {'start': 42, 'end': 57, 'diff': 43},
    'bh': {'start': 42, 'end': 57, 'diff': 44},
    'blank': {'start': 42, 'end': 57, 'diff': 43},
    'trapblank': {'start': 42, 'end': 57, 'diff': 42}
}  # sheet indexes for pulling pas and rts

compound_windows_1 = (
    {'ethane': (1.65, 1.85),  # compound retention windows for every named compound at Summit 'name':(low, high)
     'ethene': (2.04, 2.152),
     'propane': (2.85, 3.1),
     'propene': (5.75, 5.95),
     'i-butane': (7.3, 7.68),
     '4b': (.3, .5),  # 4b, acet and n-butane are not addressed by RT, but rather RT difference from i-but
     'acetylene': (.6, .725),
     'n-butane': (.56, .6),
     '5a': (9.5, 10.3),
     '5b': (11.25, 11.65),
     '5c': (12.48, 12.55),
     'i-pentane': (12.60, 12.78),
     'cfc': (12.80, 13),
     'n-pentane': (13, 13.4),
     'hexane': (16.9, 17.05),
     'benzene': (19.95, 20.25),
     'toluene': (23.3, 23.75)})

compound_windows_2 = (
    {'ethane': (1.6, 1.8),  # compound retention windows for every named compound at Summit 'name':(low, high)
     'ethene': (1.95, 2.1),
     'propane': (2.6, 3.1),
     'propene': (5.32, 5.58),
     'i-butane': (6.85, 7.1),
     '4b': (.25, .4),  # 4b, acet and n-butane are not addressed by RT, but rather RT difference from i-but
     'acetylene': (.425, .51),
     'n-butane': (.51, .6),
     '5a': (9, 9.5),
     '5b': (10.6, 11.1),
     '5c': (11.75, 11.95),
     'i-pentane': (11.95, 12.13),
     'cfc': (12.13, 12.32),
     'n-pentane': (12.35, 12.7),
     'hexane': (16.2, 16.45),
     'benzene': (19.3, 19.7),
     'toluene': (22.55, 22.95)})


class CompoundWindow(Base):
    """
    A CompoundWindow is the retention times for a set a compounds, that applies to integrations between it's start and
    end dates.
    Integration windows are a (start, end) retention time tuple, and are kept as a mutable dictionary.
    """

    __tablename__ = 'compound_windows'

    id = Column(Integer, primary_key=True)
    date_start = Column(DateTime)
    date_end = Column(DateTime)
    compounds = Column(MutableDict.as_mutable(JDict))

    def __init__(self, date_start, date_end, compounds):
        self.date_start = date_start
        self.date_end = date_end
        self.compounds = compounds


class Crf(Base):
    """
    A crf is a set of carbon response factors for compounds, tied to a datetime and standard.
    These are assigned to GcRuns, which allows them to be integrated.
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

    Peaks are related to NmhcLines, Logs, GcRuns, and Datums to make accessing them at different data levels easy.
    Revision and QC attributes have been added but are relatively unused at this point.
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
    correction_id = Column(Integer, ForeignKey('nmhc_corrections.id'))

    log_id = Column(Integer, ForeignKey('logfiles.id'))
    log_con = relationship('LogFile', foreign_keys=[log_id], back_populates='peaks')

    run_id = Column(Integer, ForeignKey('gcruns.id'))
    run_con = relationship('GcRun', foreign_keys=[run_id], back_populates='peaks')

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


class NmhcLine(Base):
    """
    A line in NMHC_PA.LOG, which contains a datetime and some set of peaks.

    When NmhcLines are matched to a LogFile, they create a GcRun (a complete run on the GC), and are considered
    'married' so they cannot be re-matched.
    """

    __tablename__ = 'nmhclines'

    id = Column(Integer, primary_key=True)
    date = Column(DateTime, unique=True)
    peaklist = relationship('Peak', order_by=Peak.id)
    status = Column(String)

    run_con = relationship('GcRun', uselist=False, back_populates='nmhc_con')

    log_con = relationship('LogFile', back_populates='line_con')

    data_con = relationship('Datum', back_populates='line_con')

    correction_id = Column(Integer, ForeignKey('nmhc_corrections.id'))
    correction = relationship('NmhcCorrection', foreign_keys=[correction_id], uselist=False, back_populates='nmhcline')

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


class NmhcCorrection(Base):
    """
    Similar to an NmhcLine, but loaded manually and created with peak corrections from Excel spreadsheets.

    Corrections can exist without a relationship to a line to prevent load-failures, but in general every Correction
    should be applied to a line.
    """

    __tablename__ = 'nmhc_corrections'

    id = Column(Integer, primary_key=True)
    date = Column(DateTime, unique=True)
    peaklist = relationship('Peak', order_by=Peak.id)
    flag = Column(Integer)  # undetermined flagging system...
    status = Column(String)
    samplecode = Column(Integer)

    nmhcline = relationship(NmhcLine, uselist=False, back_populates='correction')

    def __init__(self, nmhcline, peaks, flag, samplecode):
        self.peaklist = peaks
        self.nmhcline = nmhcline
        self.status = 'unapplied'  # all corrections are created as unapplied
        self.flag = flag
        if nmhcline:
            self.date = nmhcline.date
        self.samplecode = samplecode

    def __str__(self):
        return f'<NmhcCorrection for {self.date} with {len(self.peaklist)} peaks>'

    def __repr__(self):
        return f'<NmhcCorrection for {self.date} with {len(self.peaklist)} peaks>'


class LogFile(Base):
    """
    The output from the LabView VI, it consists of all the listed parameters recorded in the file, and is related to an
    NmhcLine, and a GcRun (if all ran correctly), and a Datum if it's an integrated ambient sample.
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
    WTA_hottemp = Column(Float)
    WTB_hottemp = Column(Float)
    GCoventemp = Column(Float)
    status = Column(String)

    run_con = relationship('GcRun', uselist=False, back_populates='log_con')

    line_id = Column(Integer, ForeignKey('nmhclines.id'))
    line_con = relationship('NmhcLine', uselist=False, foreign_keys=[line_id], back_populates='log_con')

    data_con = relationship('Datum', uselist=False, back_populates='log_con')

    peaks = relationship('Peak', back_populates='log_con')

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
    A run consists of the attributes taken from the NmhcLine and LogFile that are used to create it. This is now a
    confirmed run, meaning it was executed in PeakSimple and the VI without any fatal errors.

    When integrated, all the peak objects of a GcRun will gain a mixing ratio, and a Datum will be created and related
    to the GcRun it originated from.
    """

    __tablename__ = 'gcruns'

    id = Column(Integer, primary_key=True)
    type = Column(String)
    date = Column(DateTime)

    nmhcline_id = Column(Integer, ForeignKey('nmhclines.id'))
    nmhc_con = relationship('NmhcLine', uselist=False, foreign_keys=[nmhcline_id], back_populates='run_con')

    peaks = relationship('Peak', back_populates='run_con')
    date_end = association_proxy('nmhc_con', 'date')  # pass date from NMHC to GcRun

    logfile_id = Column(Integer, ForeignKey('logfiles.id'))
    log_con = relationship('LogFile', uselist=False, foreign_keys=[logfile_id], back_populates='run_con')

    date_start = association_proxy('log_con', 'date')  # pass date from LogFile to GcRun

    for attr in log_params_list:
        vars()[attr] = association_proxy('log_con', attr)  # set association_proxy(s)
    # for all log parameters to pass them to this GC instance

    data_con = relationship('Datum', uselist=False, back_populates='run_con')

    crfs = relationship('Crf', uselist=False)
    crf_id = Column(Integer, ForeignKey('crfs.id'))

    def __init__(self, LogFile, NmhcLine):
        self.nmhc_con = NmhcLine
        self.log_con = LogFile
        self.data_con = None
        self.crfs = None  # begins with no crf, will be found later
        self.type = sample_types.get(self.sampletype, None)
        self.peaks = NmhcLine.peaklist

        self.date = self.log_con.date + (self.nmhc_con.date - self.log_con.date) / 2

    # date of GcRun is the average of the log date and paline date

    def __str__(self):
        return f'<matched gc run at {self.date_end}>'

    def _repr__(self):
        return f'<matched gc run at {self.date_end}>'

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
    A Datum is an integrated GcRun. Integrating a GcRun automatically creates a Datum that only needs to be merged with
    the session and committed. It shares the same relationships that the NmhcLine, LogFile and GcRun that created it
    came with.

    Datum are set apart from GcRuns by having mixing ratios (after integration/creation), and by sample type. Only
    ambient samples and blanks will become Datums, since standards and trap blanks are not integrated by default.
    """

    __tablename__ = 'data'

    id = Column(Integer, primary_key=True)
    revision = Column(Integer)
    qc = Column(Integer)
    notes = Column(String)

    run_id = Column(Integer, ForeignKey('gcruns.id'))
    run_con = relationship('GcRun', uselist=False, foreign_keys=[run_id], back_populates='data_con')

    log_id = Column(Integer, ForeignKey('logfiles.id'))
    log_con = relationship('LogFile', uselist=False, foreign_keys=[log_id], back_populates='data_con')

    line_id = Column(Integer, ForeignKey('nmhclines.id'))
    line_con = relationship('NmhcLine', uselist=False, foreign_keys=[line_id], back_populates='data_con')

    for attr in gcrun_params_list:
        vars()[attr] = association_proxy('run_con', attr)  # set association_proxy(s)

    # for all log parameters to pass them to this GC instance

    def __init__(self, GcRun):
        self.run_con = GcRun
        self.revision = 0  # init with revision status zero
        self.qc = 0
        self.notes = None
        self.log_con = GcRun.log_con
        self.line_con = GcRun.nmhc_con

    def __str__(self):
        return f'<data for {self.date_end} with {len(self.peaks)} peaks>'

    def __repr__(self):
        return f'<data for {self.date_end} with {len(self.peaks)} peaks>'

    def get_crf(self, compound_name):
        return self.crfs.compounds.get(compound_name, None)

    def reintegrate(self):
        if self.crfs is None:
            return None  # no crfs, no integration!
        elif self.type == 'ambient' or self.type == 'zero':
            for peak in self.peaks:
                if peak.name in compound_list and peak.name in self.crfs.compounds.keys():
                    if peak.pa is None:
                        peak.mr = None
                    else:
                        crf = self.crfs.compounds[peak.name]

                        peak.mr = ((peak.pa /
                                    (crf * compound_ecns[peak.name] * self.sampletime * self.sampleflow1))
                                   * 1000 * 1.5)
                        # formula is (pa / (CRF * ECN * SampleTime * SampleFlow1)) * 1000 *1.5
                        # 1000 * 1.5 normalizes to a sample volume of 2000s by convention

            return None

        else:
            return None  # don't integrate if it's not an ambient or blank sample


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
            log.line_con = matched_line
            log.peaks = matched_line.peaklist
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

    peak_attr = getattr(Peak, info, None)

    if peak_attr is None:
        return None, None

    if date_start is None and date_end is None:
        try:
            peak_info = (session.query(GcRun.date, peak_attr)
                         .join(Peak, GcRun.id == Peak.run_id)
                         .filter(Peak.name == compound, GcRun.type == 'ambient')
                         .order_by(GcRun.date))  # get everything
            dates, info = zip(*peak_info.all())
        except ValueError:
            info = None
            dates = None

        return dates, info

    elif date_start is None:
        try:
            peak_info = (session.query(GcRun.date, peak_attr)
                         .join(Peak, GcRun.id == Peak.run_id)
                         .filter(Peak.name == compound, GcRun.type == 'ambient')
                         .filter(GcRun.date < date_end)
                         .order_by(GcRun.date))  # get only before the end date given

            dates, info = zip(*peak_info.all())
        except ValueError:
            info = None
            dates = None

        return dates, info

    elif date_end is None:
        try:
            peak_info = (session.query(GcRun.date, peak_attr)
                         .join(Peak, GcRun.id == Peak.run_id)
                         .filter(Peak.name == compound, GcRun.type == 'ambient')
                         .filter(GcRun.date > date_start)
                         .order_by(GcRun.date))

            dates, info = zip(*peak_info.all())  # get only after the start date given
        except ValueError:
            info = None
            dates = None

        return dates, info

    else:
        try:
            peak_info = (session.query(GcRun.date, peak_attr)
                         .join(Peak, GcRun.id == Peak.run_id)
                         .filter(Peak.name == compound, GcRun.type == 'ambient')
                         .filter(GcRun.date.between(date_start, date_end))
                         .order_by(GcRun.date))  # get between date bookends (inclusive beginning!)

            dates, info = zip(*peak_info.all())
        except ValueError:
            info = None
            dates = None

        return dates, info


def summit_voc_plot(dates, compound_dict, limits=None, minor_ticks=None, major_ticks=None,
                    y_label_str='Mixing Ratio (ppbv)'):
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
    ax.set_title(f'Summit {comp_list}', fontsize=24, y=1.02)
    ax.legend(compound_dict.keys())

    f1.subplots_adjust(bottom=.20)

    plot_name = f'{fn_list}_last_week.png'
    f1.savefig(plot_name, dpi=150)
    plt.close(f1)

    return plot_name


def summit_log_plot(name, dates, compound_dict, limits=None, minor_ticks=None, major_ticks=None,
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
                              .replace(' ', '_')
                              .replace(',', '').lower())

    comp_list = ', '.join(compound_dict.keys())  # use real names for plot title

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

    f1.savefig(name, dpi=150)
    plt.close(f1)

    return


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


def name_summit_peaks(nmhcline, rt_windows):
    """
    'Simple' peak identification based on retention times. The C4 compounds get more rigorous treatment, though.
    :param nmhcline: NmhcLine object
    :return: NmhcLine object, with named peaks
    """

    from summit_core import search_for_attr_value

    compound_pools = dict()

    for peak in nmhcline.peaklist:
        for compound, limits in rt_windows.compounds.items():
            if limits[0] < peak.rt < limits[1]:
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
                chosen.name = name

    ibut = search_for_attr_value(nmhcline.peaklist, 'name', 'i-butane')

    find_nbut = True
    find_acet = True  # default to all needing to be found
    find_4b = True

    if ibut is not None:
        ibut_rt = ibut.rt

        if find_4b:
            b4_limits = rt_windows.compounds.get('4b')
            if b4_limits:
                b4_pool = [peak for peak in nmhcline.peaklist if
                           b4_limits[0] < (peak.rt - ibut_rt) < b4_limits[1]]
            else:
                b4_pool = None
            if not b4_pool:
                pass
            else:
                b4 = max(b4_pool, key=lambda peak: peak.pa)  # get largest peak in possible peaks
                b4.name = '4b'

        if find_acet:
            acet_limits = rt_windows.compounds.get('acetylene')
            if acet_limits:
                acet_pool = [peak for peak in nmhcline.peaklist if
                             acet_limits[0] < (peak.rt - ibut_rt) < acet_limits[1]]
            else:
                acet_pool = None
            if not acet_pool:
                pass
            else:
                acet = max(acet_pool, key=lambda peak: peak.pa)  # get largest peak in possible peaks
                acet.name = 'acetylene'

        if find_nbut:
            nbut_limits = rt_windows.compounds.get('n-butane')
            if nbut_limits:
                nbut_pool = [peak for peak in nmhcline.peaklist
                             if nbut_limits[0] < (peak.rt - ibut_rt) < nbut_limits[1]]
            else:
                nbut_pool = None
            if not nbut_pool:
                pass
            else:
                nbut = max(nbut_pool, key=lambda peak: peak.pa)  # get largest peak in possible peaks
                nbut.name = 'n-butane'

    return nmhcline


def check_sheet_cols(name):
    """
    Callable mini-function passed to pd.read_excel(usecols=function).
    Grab the first column to use as an index, then all columns starting with column 4.
    """
    return True if name == 0 or name >= 4 else False


def correction_from_df_column(col, logfiles, nmhc_lines, gc_runs, logger, sheetname, codes_in_db):
    """
    Loop over columns of ambient_results_master.xlsx.
    This finds the log for each column, then retrieves the NmhcLine that matches it (if any),
    and applies all flags necessary while creating the NmhcCorrection objects.
    """
    code = col.iloc[0]

    if code in codes_in_db:
        return None  # don't touch ones that have already been added to db

    log = logfiles.filter(LogFile.samplecode == code).one_or_none()

    if not log:
        logger.warning(f'A log with samplecode {code} was not found.')
        run, line = (None, None)

    else:
        run = gc_runs.filter(GcRun.logfile_id == log.id).one_or_none()

        if not run:
            logger.warning(f'A run matching the log with samplecode {code} was not found.')
            line = None
        else:
            line = nmhc_lines.filter(NmhcLine.id == run.nmhcline_id).one_or_none()

    correction_peaklist = []

    slice_st = sheet_slices.get(sheetname).get('start')
    slice_end = sheet_slices.get(sheetname).get('end')
    diff = sheet_slices.get(sheetname).get('diff')

    for name, pa, rt in zip(col.index[slice_st:slice_end].tolist(),
                            col[slice_st:slice_end].tolist(),
                            col[slice_st + diff:slice_end + diff].tolist()):
        correction_peaklist.append(Peak(name, pa, rt))

    if not correction_peaklist:
        return None

    return NmhcCorrection(line, correction_peaklist, None, code)


def find_approximate_rt(peaklist, rt):
    peaklist = [peak for peak in peaklist if peak.rt]  # clean list for only those with RTs
    return next((peak for peak in peaklist if rt - .011 < peak.rt < rt + .011), None)
