import os, asyncio
import pandas as pd
import datetime as dt
from summit_picarro import logger, rundir

"""
Things Needed:

File checking:
	Needs to:
		Check for new or updated files
		Read new files
		
Calibration Handling:
	Needs to:
		Search all data, then new data
		Pick out all calibration data
		Separate into low, middle, high standards based on valves
			Group in a single calibration event and calculate some things based on the regression, etc
		Potentially apply information from the above on data
		
Plotting:
	Needs to:
		Plot data if new data is available
		

"""


async def fake_move_data(directory, sleeptime):
	"""
	Moves files from /test_data to /data to simulate incoming data transfers.
	I didn't bother simulating the correct directory structures like 2019/3/11/file_on_20190311.dat because
	get_all_data_files() relies on Path.rglob(), which essentially ignores that structure.

	:param directory: main program directory
	:param sleeptime: seconds to sleep between file moves
	:return: None
	"""
	while True:
		from summit_picarro import get_all_data_files
		from shutil import copy2

		local_files = get_all_data_files(directory / 'data')
		remote_files = get_all_data_files(directory / 'test_data')

		local_filenames = [f.name for f in local_files]
		remote_filenames = [f.name for f in remote_files]

		for file, filename in zip(remote_files, remote_filenames):
			if filename not in local_filenames:
				copy2(file, directory / 'data' / filename)
				logger.info(f'Moved file {filename} to local directory.')
				await asyncio.sleep(sleeptime)

		await asyncio.sleep(sleeptime)

async def check_load_new_data(directory, sleeptime):

	while True:
		logger.info('Running check_load_new_data()')
		from summit_picarro import connect_to_db, get_all_data_files, DataFile, Datum, check_filesize

		engine, session, Base = connect_to_db('sqlite:///summit_picarro.sqlite', directory)
		Base.metadata.create_all(engine)

		db_files = session.query(DataFile)
		db_data = session.query(Datum)

		db_filenames = [d.name for d in db_files.all()]
		db_dates = [d.date for d in db_data.all()]

		all_available_files = get_all_data_files(directory / 'data')

		files_to_process = session.query(DataFile).filter(DataFile.processed == False).all()
		# start with list of unprocessed files
		from sqlalchemy.orm.exc import MultipleResultsFound

		for file in all_available_files:
			try:
				db_match = db_files.filter(DataFile._name == file.name).one_or_none()
			except MultipleResultsFound:
				logger.warning(f'Multiple results found for file {file.name}. The first was used.')
				db_match = db_files.filter(DataFile._name == file.name).first()

			if file.name not in db_filenames:
				files_to_process.append(DataFile(file))
			elif check_filesize(file) > db_match.size:
				# if a matching file was found and it's now bigger, append for processing
				logger.info(f'File {file.name} had more data and was added for procesing.')
				files_to_process.append(db_match)
			else:
				pass

		if len(files_to_process) is 0:
			logger.warning('No new data was found.')
			await asyncio.sleep(sleeptime)
			continue

		for ind, file in enumerate(files_to_process):
			files_to_process[ind] = session.merge(file)  # merge files and return the merged object to overwrite the old
			logger.info(f'File {file.name} added for processing.')
		session.commit()

		for file in files_to_process:
			df = pd.read_csv(file.path, delim_whitespace=True)
			df_list = df.to_dict('records')

			data_list = []
			for line in df_list:
				data_list.append(Datum(line))

			for d in data_list:
				if d.date not in db_dates:
					d.file_id = file.id  # relate Datum to the file it originated in
					session.add(d)

			file.processed = True
			logger.info(f'All data in file {file.name} processed.')
			session.commit()

		await asyncio.sleep(sleeptime)


async def find_cal_events(directory, sleeptime):
	while True:
		logger.info('Running find_cal_events()')
		from summit_picarro import connect_to_db, Datum, CalEvent, mpv_converter, find_cal_indices

		engine, session, Base = connect_to_db('sqlite:///summit_picarro.sqlite', directory)
		Base.metadata.create_all(engine)

		standard_data = {}
		for MPV in [2, 3, 4]:
			mpv_data = pd.DataFrame(session
									.query(Datum.id, Datum.date)
									.filter(Datum.mpv_position == MPV)
									.filter(Datum.cal_id == None)
									.all())
			# get only data for this switching valve position, and not already in any calibration event

			if len(mpv_data) is 0:
				logger.info(f'No new calibration events found for standard {mpv_converter[MPV]}')
				continue

			mpv_data['date'] = pd.to_datetime(mpv_data['date'])
			standard_data[mpv_converter[MPV]] = mpv_data.sort_values(by=['date']).reset_index(drop=True)

		for standard, data in standard_data.items():
			indices = find_cal_indices(data['date'])

			if len(indices) == 0:
				logger.info(f'No new cal events were found for {standard} standard.')
				continue

			prev_ind = 0
			cal_events = []

			for num, ind in enumerate(indices):  # get all data within this event
				event_data = session.query(Datum).filter(Datum.id.in_(data['id'].iloc[prev_ind:ind])).all()
				cal_events.append(CalEvent(event_data, standard))

				if num == len(indices):  # if it's the last index, get all ahead of it as the last event
					event_data = session.query(Datum).filter(Datum.id.in_(data['id'].iloc[ind:])).all()
					cal_events.append(CalEvent(event_data, standard))

				prev_ind = ind

			for ev in cal_events:
				if ev.date - ev.dates[0] < dt.timedelta(seconds=90):
					logger.info(f'CalEvent for date {ev.date} had a duration < 90s and was ignored.')
				else:
					session.merge(ev)
					logger.info(f'CalEvent for date {ev.date} added.')
			session.commit()

		session.close()
		engine.dispose()
		await asyncio.sleep(sleeptime)


loop = asyncio.get_event_loop()

loop.create_task(check_load_new_data(rundir, 5))
loop.create_task(fake_move_data(rundir, 4))
loop.create_task(find_cal_events(rundir, 20))

loop.run_forever()