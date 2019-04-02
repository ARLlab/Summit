"""
Methane has only CH4.LOG, and per-run logs. The .LOG file contains date / peaks (name, peak area, retention time)
This just needs to be read in, parsed for new dates/lines, then those lines need to be parsed for peaks, and
sample peaks need to be quantified on the standard peaks.

The per-run logs contain sample pressures, etc; those are associated with pairs of two runs.

Some runtime QC is needed to prevent quantification from failed standard runs, poor integrations, etc.
"""

from pathlib import Path
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

		files = get_all_data_files(directory / 'data')

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
		from summit_methane import connect_to_db, Peak, Sample, PaLine, GcRun
		from summit_methane import sample_rts
		from operator import attrgetter
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
					peak = max(potential_peaks, key=attrgetter('pa'))
					if peak is not None:
						sample.peak = peak
						session.merge(sample)

				else:
					continue

		session.commit()
		session.close()
		engine.dispose()
		await asyncio.sleep(sleeptime)


def main():
	# log_path = Path(r'C:\Users\brend\PycharmProjects\Summit\processors\summit_methane_processor\CH4_test.LOG')
	log_path = Path(r'C:\Users\arl\Desktop\summit_master\processors\summit_methane_processor\CH4_test.LOG')

	loop = asyncio.get_event_loop()

	loop.create_task(check_load_pa_log(rundir, log_path, 10))
	loop.create_task(check_load_run_logs(rundir, 10))
	loop.create_task(match_runs_to_lines(rundir, 10))
	loop.create_task(match_peaks_to_samples(rundir, 10))

	loop.run_forever()


if __name__ == '__main__':
	main()