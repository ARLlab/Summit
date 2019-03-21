"""
Methane has only CH4.LOG, and per-run logs. The .LOG file contains date / peaks (name, peak area, retention time)
This just needs to be read in, parsed for new dates/lines, then those lines need to be parsed for peaks, and
sample peaks need to be quantified on the standard peaks.

The per-run logs contain sample pressures, etc; those are associated with pairs of two runs.

Some runtime QC is needed to prevent quantification from failed standard runs, poor integrations, etc.
"""
from pathlib import Path
import asyncio

rundir = Path(r'C:\Users\brend\PycharmProjects\Summit\processors\summit_methane_processor')
#rundir = Path(r'C:\Users\arl\Desktop\summit_master\processors\summit_methane_processor')

from summit_methane import logger

async def check_load_pa_log(directory, path, sleeptime):
	while True:
		logger.info('Running check_load_pa_log()')
		from summit_methane import read_pa_line, connect_to_db, PaLine

		engine, session, Base = connect_to_db('sqlite:///summit_methane.sqlite', directory)
		Base.metadata.create_all(engine)

		lines_in_db = session.query(PaLine).all()
		dates_in_db = [l.date for l in lines_in_db]

		pa_file_contents = path.read_text().split('\n')

		pa_lines = []
		for line in pa_file_contents:
			pa_lines.append(read_pa_line(line))

		if len(pa_lines) == 0:
			logger.info('No new PA lines found.')

		else:
			for line in pa_lines:
				if line.date not in dates_in_db:
					session.add(line)
			session.commit()

		session.close()
		engine.dispose()

		await asyncio.sleep(sleeptime)



async def check_load_run_logs(directory, sleeptime):
	while True:
		from summit_methane import get_all_data_files, connect_to_db

		engine, session, Base = connect_to_db('sqlite:///summit_methane.sqlite', directory)

		files = get_all_data_files(directory / 'data')

		for file in files:
			pass


			# create GcRuns
			# add Samples



def main():
	log_path = Path(r'C:\Users\brend\PycharmProjects\Summit\processors\summit_methane_processor\CH4_test.LOG')

	loop = asyncio.get_event_loop()

	loop.create_task(check_load_pa_log(rundir, log_path, 10))

	loop.run_forever()


if __name__ == '__main__':
	main()