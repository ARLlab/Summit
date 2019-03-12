import os, random
from pathlib import Path
import pandas as pd
from summit_picarro import connect_to_db, Datum

homedir = Path(r'C:\Users\arl\Desktop\summit_master\processors\summit_picarro_processor\tests')

data_basedir = homedir / '..'

engine, session, Base = connect_to_db('sqlite:///summit_picarro.sqlite', data_basedir)

Base.metadata.create_all(engine)

# LOW, MED, HIGH == MPVs 2, 4, 3

mpv_converter = {1: 'ambient', 2: 'low_std', 4: 'mid_std', 3: 'high_std'}

attr_list = ['id', 'date', 'co', 'co2', 'ch4']

querylist = []
querylist.append([getattr(Datum, attr) for attr in attr_list])


cal_data = {}
for MPV in [2,3,4]:
	cal_data[mpv_converter[MPV]] = pd.DataFrame(session.query(*attr_list).filter(Datum.mpv_position == MPV).all())


def find_cal_indices(epoch_time):
	"""
	Cal events are any time a standard is injected and being quantified by the system. Here, they're separated as though
	any calibration data that's more than 10s away from the previous cal data is a new event.

	:param epoch_time: array of epoch times for all of the supplied data
	:return: list of cal events indices, where each index is the beginning of a new cal event
	"""
	epoch_diff = epoch_time.diff()
	indices = [i-1 for i in epoch_diff.loc[epoch_diff > 10].index.values.tolist()]  # subtract one from all indices
	return indices


class CalEvent():

	cal_count = 0  # unique ID generator

	def __init__(self, DT, CH4, CO2, CO):
		self.id = CalEvent.cal_count
		CalEvent.cal_count += 1  # increment class var to get unique IDs
		self.datetime = DT
		self.CH4 = CH4*1000
		self.CO2 = CO2
		self.CO = CO*1000
		self.plot_time = self.datetime.iat[-1]
		self.duration = self.datetime.iat[-1] - self.datetime.iat[0]
		self.cal_period = 20  # using a 20s back-window of calibration data

		self.CH4_mean = self.CH4[self.get_back_period(self.cal_period)].mean()
		self.CO2_mean = self.CO2[self.get_back_period(self.cal_period)].mean()
		self.CO_mean = self.CO[self.get_back_period(self.cal_period)].mean()

		self.CH4_med = self.CH4[self.get_back_period(self.cal_period)].median()
		self.CO2_med = self.CO2[self.get_back_period(self.cal_period)].median()
		self.CO_med = self.CO[self.get_back_period(self.cal_period)].median()

		self.CH4_std = self.CH4[self.get_back_period(self.cal_period)].std()
		self.CO2_std = self.CO2[self.get_back_period(self.cal_period)].std()
		self.CO_std = self.CO[self.get_back_period(self.cal_period)].std()


	def __str__(self):
		return f"<CalEvent {self.id} with {len(self.CH4)}, {len(self.CO2)}, {len(self.CO)}, CH4, CO2, and CO data points>"

	def __repr__(self):
		return f"<CalEvent {self.id} with {len(self.CH4)} CH4, CO2, and CO data points>"

	def get_datetime(self):
		return self.datetime

	def get_plot_time(self):
		return self.plot_time

	def get_id(self):
		return self.id

	def get_CH4(self):
		return self.CH4

	def get_CO2(self):
		return self.CO2

	def get_CO(self):
		return self.CO

	def get_mean(self, compound):
		assert compound in ('CH4', 'CO2', 'CO'), "Invalid compound given to get_mean()."
		return getattr(self, compound+'_mean', None)

	def get_median(self, compound):
		assert compound in ('CH4', 'CO2', 'CO'), "Invalid compound given to get_median()."
		return getattr(self, compound+'_med', None)

	def get_std(self, compound):
		assert compound in ('CH4', 'CO2', 'CO'), "Invalid compound given to get_std()."
		return getattr(self, compound+'_std', None)

	def get_back_period(self, seconds):
		"""Retrieve boolean array such that self.CO2[bool_array] captures the last (given) seCOnds of data in the CalEvent"""
		start_t = self.datetime.iat[-1] - pd.to_timedelta(seconds, unit='s')
		return self.datetime >= start_t
