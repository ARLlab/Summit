"""
Methane has only CH4.LOG, and per-run logs. The .LOG file contains date / peaks (name, peak area, retention time)
This just needs to be read in, parsed for new dates/lines, then those lines need to be parsed for peaks, and
sample peaks need to be quantified on the standard peaks.

The per-run logs contain sample pressures, etc; those are associated with pairs of two runs.

Some runtime QC is needed to prevent quantification from failed standard runs, poor integrations, etc.
"""
from pathlib import Path
import asyncio

rundir = Path(r'C:\Users\arl\Desktop\summit_master\processors\summit_methane_processor')


async def check_load_pa_log(rundir, sleeptime):
	while True:
		pass
		# from summit_methane import get_all_data_files
		# files = get_all_data_files(rundir / 'data')
		#
		# for file in files:
		# 	pass


async def check_load_run_logs():
	while True:
		from summit_methane import get_all_data_files
		files = get_all_data_files(rundir / 'data')

		for file in files:


			# create GcRuns
			# add Samples
