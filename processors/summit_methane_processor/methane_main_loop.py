"""
Methane has only CH4.LOG, and per-run logs. The .LOG file contains date / peaks (name, peak area, retention time)
This just needs to be read in, parsed for new dates/lines, then those lines need to be parsed for peaks, and
sample peaks need to be quantified on the standard peaks.

The per-run logs contain sample pressures, etc; those are associated with pairs of two runs.

Some runtime QC is needed to prevent quantification from failed standard runs, poor integrations, etc.
"""

from pathlib import Path
from datetime import datetime
import statistics as s
import datetime as dt
import asyncio

# rundir = Path(r'C:\Users\brend\PycharmProjects\Summit\processors\summit_methane_processor')
rundir = Path(r'C:\Users\arl\Desktop\summit_master\processors\summit_methane_processor')

from summit_methane import logger


async def check_load_pa_log(directory, path, sleeptime):

	start_size = 0  #default to checking entire file on startup
	start_line = 0

	while True:
		logger.info('Running check_load_pa_log()')
		from summit_methane import read_pa_line, connect_to_db, PaLine, check_filesize

		engine, session, Base = connect_to_db('sqlite:///summit_methane.sqlite', directory)
		Base.metadata.create_all(engine)

		lines_in_db = session.query(PaLine).all()
		dates_in_db = [l.date for l in lines_in_db]

		if check_filesize(path) <= start_size:
			logger.info('PA file did not change size.')
			session.close()
			engine.dispose()
			await asyncio.sleep(sleeptime)
			continue

		pa_file_contents = path.read_text().split('\n')[start_line:]
		pa_file_contents[:] = [line for line in pa_file_contents if line != '']
		start_line = len(pa_file_contents)

		pa_lines = []
		for line in pa_file_contents:
			pa_lines.append(read_pa_line(line))

		if len(pa_lines) == 0:
			logger.info('No new PaLines found.')

		else:
			ct = 0  # count committed logs
			for line in pa_lines:
				if line.date not in dates_in_db:
					session.add(line)
					logger.info(f'PaLine for {line.date} added.')
					ct += 1

			if ct == 0:
				logger.info('No new PaLines found.')
			else:
				logger.info(f'{ct} PaLines added.')
				session.commit()

		session.close()
		engine.dispose()

		await asyncio.sleep(sleeptime)


async def check_load_run_logs(directory, sleeptime):
	while True:
		from summit_methane import get_all_data_files, connect_to_db, GcRun, Sample, read_log_file

		engine, session, Base = connect_to_db('sqlite:///summit_methane.sqlite', directory)

		runs_in_db = session.query(GcRun).all()
		samples = session.query(Sample)
		samples_in_db = samples.all()
		sample_count = samples.count()

		run_dates = [r.date for r in runs_in_db]

		files = get_all_data_files(directory / 'data', '.txt')

		runs = []
		for file in files:
			runs.append(read_log_file(file))

		new_run_count = 0  # count runs added
		for run in runs:
			if run.date not in run_dates:
				session.add(run)
				logger.info(f'GcRun for {run.date} added.')
				new_run_count +=1

		if new_run_count == 0:
			logger.info('No new GcRuns added.')
		else:
			session.commit()
			new_sample_count = session.query(Sample).count() - sample_count
			logger.info(f'{new_run_count} GcRuns added, containing {new_sample_count} Samples.')

			if new_run_count * 10 != new_sample_count:
				logger.warning('There were not ten Samples per GcRun as expected.')

		session.close()
		engine.dispose()
		await asyncio.sleep(sleeptime)


async def match_runs_to_lines(directory, sleeptime):
	while True:
		logger.info('Running match_runs_to_peaks()')
		from summit_methane import connect_to_db, GcRun, PaLine, match_lines_to_runs

		engine, session, Base = connect_to_db('sqlite:///summit_methane.sqlite', directory)

		unmatched_lines = session.query(PaLine).filter(PaLine.run == None).all()
		unmatched_runs = session.query(GcRun).filter(GcRun.pa_line_id == None).all()

		married_runs_count = session.query(GcRun).filter(GcRun.status == 'married').count()

		lines, runs, count = match_lines_to_runs(unmatched_lines, unmatched_runs)

		session.commit()

		if count is not 0:
			logger.info(f'{count} GcRuns matched with PaLines.')
		else:
			logger.info('No new GcRun-PaLine pairs matched.')

		await asyncio.sleep(sleeptime)


async def match_peaks_to_samples(directory, sleeptime):

	while True:
		logger.info('Running match_peaks_to_samples()')
		from summit_methane import connect_to_db, Peak, Sample, PaLine, GcRun
		from summit_methane import sample_rts
		from operator import attrgetter
		import datetime as dt
		engine, session, Base = connect_to_db('sqlite:///summit_methane.sqlite', directory)

		unmatched_samples = session.query(Sample).filter(Sample.peak_id == None, Sample.run_id != None).all()

		runs_w_unmatched_samples = (session.query(GcRun)
									.filter(GcRun.id.in_({s.run_id for s in unmatched_samples}))
									.all())  # create set of runs that require processing

		for run in runs_w_unmatched_samples:
			# loop through runs containing samples that haven't been matched with peaks
			samples = session.query(Sample).filter(Sample.run_id == run.id).all()
			peaks = session.query(Peak).filter(Peak.pa_line_id == run.pa_line_id)

			for sample in samples:
				sn = sample.sample_num
				potential_peaks = peaks.filter(Peak.rt.between(sample_rts[sn][0], sample_rts[sn][1])).all()
				# filter for peaks in this gc run between the expected retention times given in sample_rts

				if len(potential_peaks) > 0:
					# currently, the criteria for "this is the real peak" is "this is the biggest peak"
					peak = max(potential_peaks, key=attrgetter('pa'))
					if peak is not None:
						sample.peak = peak
						peak.name = 'CH4_' + str(sample.sample_num)
						sample.date = run.pa_line.date + dt.timedelta(minutes=peak.rt - 1)
						session.merge(sample)

				else:
					continue

		session.commit()
		session.close()
		engine.dispose()
		await asyncio.sleep(sleeptime)


async def add_one_standard(directory, sleeptime):
	"""
	Add a single standard (the current working one), so that quantifications are possible. VERY TEMPORARY.

	:param directory:
	:param sleeptime:
	:return:
	"""
	while True:
		from summit_methane import Standard, connect_to_db

		engine, session, Base = connect_to_db('sqlite:///summit_methane.sqlite', directory)

		my_only_standard = Standard('ws_2019', 2067.16, datetime(2019, 1, 1), datetime(2019, 6, 1))
		session.merge(my_only_standard)
		session.commit()

		session.close()
		engine.dispose()
		await asyncio.sleep(sleeptime)


async def quantify_samples(directory, sleeptime):
	"""
	On a per-run basis, use std1 to calc samples 1-5 (~3) and std2 to calculate samples 6-10 (~8)

	:param directory:
	:param sleeptime:
	:return:
	"""

	while True:
		logger.info('Running quantify_samples()')
		from summit_methane import connect_to_db, GcRun, Sample, Peak, Standard
		from summit_methane import search_for_attr_value, calc_ch4_mr, valid_sample

		engine, session, Base = connect_to_db('sqlite:///summit_methane.sqlite', directory)

		unquantified_runs = session.query(GcRun).filter(GcRun.median == None).all()

		ct = 0
		for run in unquantified_runs:
			samples = run.samples

			standard = (session.query(Standard)
						.filter(run.date >= Standard.date_st, run.date < Standard.date_en)
						.one_or_none())

			if standard is not None:
				ambients = [sample for sample in samples if (sample.sample_type == 3 and valid_sample(sample))]
				standard1 = search_for_attr_value(samples, 'sample_num', 2)
				standard2 = search_for_attr_value(samples, 'sample_num', 7)

				if len(ambients) is 0:
					logger.warning(f'No ambient samples were quantifiable in GcRun for {run.date}')
					continue

				if (not valid_sample(standard1)) and (not valid_sample(standard2)):
					logger.warning(f'No valid standard samples found in GcRun for {run.date}.')
					continue

				elif not valid_sample(standard1):
					# use std2 for all ambient quantifications
					logger.info(f'Only one standard used for samples in GcRun for {run.date}')
					for amb in ambients:
						amb = calc_ch4_mr(amb, standard2, standard)

				elif not valid_sample(standard2):
					# use std1 for all ambient quantifications
					logger.info(f'Only one standard used for samples in GcRun for {run.date}')
					for amb in ambients:
						amb = calc_ch4_mr(amb, standard1, standard)

				else:
					# use std1 for ambients 0-4 and std2 for ambients 5-9
					for amb in ambients:
						if amb.sample_num < 5:
							amb = calc_ch4_mr(amb, standard1, standard)
						else:
							amb = calc_ch4_mr(amb, standard2, standard)

					run.standard_rsd = (s.stdev([standard1.peak.pa, standard2.peak.pa]) /
										s.median([standard1.peak.pa, standard2.peak.pa])
										* 100)

				all_run_mrs = [amb.peak.mr for amb in ambients]

				run.median = s.median(all_run_mrs)
				run.rsd = s.stdev(all_run_mrs) / run.median * 100

				session.merge(run)
				# merge only the run, it contains and cascades samples, palines and peaks that were changed
				ct += 1

			else:
				logger.warning(f'No standard value found for GcRun at {run.date}.')

		session.commit()

		if ct > 0:
			logger.info(f'{ct} GcRuns were successfully quantified.')
		else:
			logger.info('No GcRuns quantified.')

		session.close()
		engine.dispose()
		await asyncio.sleep(sleeptime)


async def plot_new_data(directory, sleeptime):

	data_len = 0  # always default to run when initialized

	while True:
		logger.info('Running plot_new_data()')
		from summit_methane import connect_to_db, Sample, plottable_sample, summit_methane_plot

		engine, session, Base = connect_to_db('sqlite:///summit_methane.sqlite', directory)

		ambient_samples = (session.query(Sample)
						   .filter(Sample.date.between(datetime(2019,1,1), datetime(2019,6,1)))
						   .filter(Sample.sample_type == 3)
						   .all())

		ambient_samples[:] = [sample for sample in ambient_samples if plottable_sample(sample)]
		# remove gross outliers and non-valid samples

		if len(ambient_samples) > data_len:
			ambient_dates = [amb.date for amb in ambient_samples]
			ambient_mrs = [amb.peak.mr for amb in ambient_samples]

			summit_methane_plot(ambient_dates, {'methane': [None, ambient_mrs]},
								limits={'bottom': 1800, 'top': 2150})
			logger.info('New data plots created.')
		else:
			logger.info('No new data found to be plotted.')

		data_len = len(ambient_samples)

		session.close()
		engine.dispose()
		await asyncio.sleep(sleeptime)




def main():
	# log_path = Path(r'C:\Users\brend\PycharmProjects\Summit\processors\summit_methane_processor\CH4_test.LOG')
	log_path = Path(r'C:\Users\arl\Desktop\summit_master\processors\summit_methane_processor\CH4_smooth.LOG')

	loop = asyncio.get_event_loop()

	loop.create_task(check_load_pa_log(rundir, log_path, 10))
	loop.create_task(check_load_run_logs(rundir, 10))
	loop.create_task(match_runs_to_lines(rundir, 10))
	loop.create_task(match_peaks_to_samples(rundir, 10))
	loop.create_task(add_one_standard(rundir, 120))
	loop.create_task(quantify_samples(rundir, 10))

	loop.run_forever()


if __name__ == '__main__':
	main()