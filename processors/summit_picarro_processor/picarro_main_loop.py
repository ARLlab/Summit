import asyncio
import datetime as dt
import pandas as pd
from summit_errors import send_processor_email

# async def fake_move_data(directory, sleeptime):
# 	"""
# 	Moves files from /test_data to /data to simulate incoming data transfers.
# 	I didn't bother simulating the correct directory structures like 2019/3/11/file_on_20190311.dat because
# 	get_all_data_files() relies on Path.rglob(), which essentially ignores that structure.
#
# 	:param directory: main program directory
# 	:param sleeptime: seconds to sleep between file moves
# 	:return: None
# 	"""
# 	while True:
# 		from summit_core import get_all_data_files
# 		from shutil import copy2
#
# 		local_files = get_all_data_files(directory / 'data', '.dat')
# 		remote_files = get_all_data_files(directory / 'test_data', '.dat')
#
# 		local_filenames = [f.name for f in local_files]
# 		remote_filenames = [f.name for f in remote_files]
#
# 		for file, filename in zip(remote_files, remote_filenames):
# 			if filename not in local_filenames:
# 				copy2(file, directory / 'data' / filename)
# 				logger.info(f'Moved file {filename} to local directory.')
# 				await asyncio.sleep(sleeptime)
#
# 		await asyncio.sleep(sleeptime)

PROC = 'Picarro Processor'


async def check_load_new_data(logger):
    """
    Checks for new files, checks length of old ones for updates, and processes/commits new data to the database.

    :param logger: logging logger at module level
    :return: boolean, did it run/process new data?
    """

    logger.info('Running check_load_new_data()')

    try:
        from summit_core import picarro_logs_path as data_path
        from summit_core import picarro_dir as rundir
        from summit_core import connect_to_db, get_all_data_files, check_filesize
        from summit_picarro import Base, DataFile, Datum
        from sqlalchemy.orm.exc import MultipleResultsFound
    except ImportError as e:
        logger.error('ImportError occurred in check_load_new_data()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_picarro.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Exception {e.args} caused database connection to fail in check_load_new_data()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        db_files = session.query(DataFile)
        db_data = session.query(Datum)

        db_filenames = [d.name for d in db_files.all()]
        db_dates = [d.date for d in db_data.all()]

        all_available_files = get_all_data_files(data_path, '.dat')

        files_to_process = session.query(DataFile).filter(DataFile.processed == False).all()

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

        if not len(files_to_process):
            logger.warning('No new data was found.')
            return False

        for ind, file in enumerate(files_to_process):
            files_to_process[ind] = session.merge(file)  # merge files and return the merged object to overwrite the old
            logger.info(f'File {file.name} added for processing.')
        session.commit()

        for file in files_to_process:
            df = pd.read_csv(file.path, delim_whitespace=True)
            # CO2 stays in ppm
            df['CO_sync'] *= 1000  # convert CO to ppb
            df['CH4_sync'] *= 1000  # convert CH4 to ppb
            df['CH4_dry_sync'] *= 1000

            df_list = df.to_dict('records')  # convert to list of dicts

            data_list = []
            for line in df_list:
                data_list.append(Datum(line))

            if data_list:
                for d in data_list:
                    if d.date not in db_dates:
                        d.file_id = file.id  # relate Datum to the file it originated in
                        session.add(d)
            else:
                logger.info(f'No new data created from file {file.name}.')

            file.processed = True
            logger.info(f'All data in file {file.name} processed.')
            session.commit()

        return True

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in check_load_new_data().')
        send_processor_email(PROC, exception=e)
        return False


async def find_cal_events(logger):
    """
    Searches the existing data for unused calibration data and creates/commits CalEvents if possible.

    :param logger: logging logger at module level
    :return: boolean, did it run/process new data?
    """

    logger.info('Running find_cal_events()')
    try:
        from summit_core import connect_to_db
        from summit_core import picarro_dir as rundir
        from summit_picarro import Base, Datum, CalEvent, mpv_converter, find_cal_indices
        from summit_picarro import log_event_quantification
    except Exception as e:
        logger.error('ImportError occured in find_cal_events()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_picarro.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Exception {e.args} occurred in find_cal_events()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        standard_data = {}
        for MPV in [2, 3, 4]:
            mpv_data = pd.DataFrame(session
                                    .query(Datum.id, Datum.date)
                                    .filter(Datum.mpv_position == MPV)
                                    .filter(Datum.cal_id == None)
                                    .all())
            # get only data for this switching valve position, and not already in any calibration event

            if not len(mpv_data):
                logger.info(f'No new calibration events found for standard {mpv_converter[MPV]}')
                continue

            mpv_data['date'] = pd.to_datetime(mpv_data['date'])
            # use mpv_converter dict to get standard information
            standard_data[mpv_converter[MPV]] = mpv_data.sort_values(by=['date']).reset_index(drop=True)

        for standard, data in standard_data.items():
            indices = find_cal_indices(data['date'])

            if not len(indices):
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
                    ev.standard_used = 'dump'  # give not-long-enough events standard type 'dump' so they're ignored
                    session.merge(ev)
                else:
                    for cpd in ['co', 'co2', 'ch4']:
                        ev.calc_result(cpd, 21)  # calculate results for all compounds going 21s back

                    session.merge(ev)
                    logger.info(f'CalEvent for date {ev.date} added.')
                    log_event_quantification(logger, ev)  # show quantification info as DEBUG in log
            session.commit()
        return True

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in find_cal_events()')
        send_processor_email(PROC, exception=e)
        return False


async def create_mastercals(logger):
    """
    Searches all un-committed CalEvents, looking for (high, middle, low) sets that can then have a curve and
    other stats calculated. It will report them as DEBUG items in the log.

    :param logger: logging logger at module level
    :return: boolean, did it run/process new data?
    """

    try:
        from summit_core import picarro_dir as rundir
        from summit_core import connect_to_db
        from summit_picarro import MasterCal, CalEvent, match_cals_by_min
    except Exception as e:
        logger.error('ImportError occured in create_mastercals()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_picarro.sqlite', rundir)
    except Exception as e:
        logger.error(f'Exception {e.args} prevented connection to database in create_mastercals()')
        send_processor_email(PROC, exception=e)
        return False
    try:
        # Get cals by standard, but only if they're not in another MasterCal already
        lowcals = (session.query(CalEvent)
                   .filter(CalEvent.mastercal_id == None, CalEvent.standard_used == 'low_std')
                   .all())

        highcals = (session.query(CalEvent)
                    .filter(CalEvent.mastercal_id == None, CalEvent.standard_used == 'high_std')
                    .all())

        midcals = (session.query(CalEvent)
                   .filter(CalEvent.mastercal_id == None, CalEvent.standard_used == 'mid_std')
                   .all())

        mastercals = []
        for lowcal in lowcals:
            matching_high = match_cals_by_min(lowcal, highcals, minutes=5)

            if matching_high is not None:
                matching_mid = match_cals_by_min(matching_high, midcals, minutes=5)

                if matching_mid is not None:
                    mastercals.append(MasterCal([lowcal, matching_high, matching_mid]))

        if mastercals:
            for mc in mastercals:
                mc.create_curve()  # calculate curve from low - high point, and check middle distance
                session.add(mc)
                logger.info(f'MasterCal for {mc.subcals[0].date} created.')

            session.commit()
            return True
        else:
            logger.info('No MasterCals were created.')
            return False

    except Exception as e:
        logger.error(f'Exception {e.args} occured in create_mastercals()')
        send_processor_email(PROC, exception=e)
        return False


async def plot_new_data(logger):
    """
    Checks data against the last plotting time, and creates new plots for CO, CO2, and CH4 if new data exists.

    :param logger: logging logger at module level
    :return: boolean, did it run/process new data?
    """

    logger.info('Running plot_new_data()')

    try:
        from summit_core import picarro_dir as rundir
        from summit_core import create_daily_ticks, connect_to_db, TempDir, Plot, core_dir, Config
        from summit_picarro import Base, Datum, summit_picarro_plot
    except Exception as e:
        logger.error('ImportError occurred in plot_new_data()')
        send_processor_email(PROC, exception=e)
        return False

    plotdir = rundir / 'plots'

    try:
        engine, session = connect_to_db('sqlite:///summit_picarro.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Exception {e.args} occurred in plot_new_data()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        core_engine, core_session = connect_to_db('sqlite:///summit_core.sqlite', core_dir)
        Plot.__table__.create(core_engine, checkfirst=True)
        Config.__table__.create(core_engine, checkfirst=True)

        picarro_config = core_session.query(Config).filter(Config.processor == PROC).one_or_none()

        if not picarro_config:
            picarro_config = Config(processor=PROC)  # use all default values except processor on init
            core_session.add(picarro_config)
            core_session.commit()

    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the core database in plot_new_data()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        newest_data_point = (session.query(Datum.date)
            .filter(Datum.mpv_position == 1)
            .order_by(Datum.date.desc()).first()[0])

        if newest_data_point <= picarro_config.last_data_date:
            logger.info('No new data was found to plot.')
            core_session.close()
            core_engine.dispose()
            session.close()
            engine.dispose()
            return False

        picarro_config.last_data_date = newest_data_point
        core_session.add(picarro_config)

        date_limits, major_ticks, minor_ticks = create_daily_ticks(picarro_config.days_to_plot)

        all_data = (session.query(Datum.date, Datum.co, Datum.co2, Datum.ch4)
                    .filter(Datum.mpv_position == 0 | Datum.mpv_position == 1)
                    .filter(Datum.instrument_status == 963, Datum.alarm_status == 0)
                    .filter(Datum.date >= date_limits['left'])  # grab only data that falls in plotting period
                    .all())

        if not all_data:
            logger.info('No new data was found to plot.')
            core_session.close()
            core_engine.dispose()
            session.close()
            engine.dispose()
            return False

        # get only ambient data
        dates = []
        co = []
        co2 = []
        ch4 = []
        for result in all_data:
            dates.append(result.date)
            co.append(result.co)
            co2.append(result.co2)
            ch4.append(result.ch4)

        with TempDir(plotdir):
            name = summit_picarro_plot(None, ({'Summit CO': [dates, co]}),
                                limits={'right': date_limits.get('right', None),
                                        'left': date_limits.get('left', None),
                                        'bottom': 0,
                                        'top': 300},
                                major_ticks=major_ticks,
                                minor_ticks=minor_ticks)

            co_plot = Plot(plotdir / name, True)  # stage plots to be uploaded
            core_session.add(co_plot)

            name = summit_picarro_plot(None, ({'Summit CO2': [dates, co2]}),
                                limits={'right': date_limits.get('right', None),
                                        'left': date_limits.get('left', None),
                                        'bottom': 400,
                                        'top': 500},
                                major_ticks=major_ticks,
                                minor_ticks=minor_ticks,
                                unit_string='ppmv')

            co2_plot = Plot(plotdir / name, True)  # stage plots to be uploaded
            core_session.add(co2_plot)

            name = summit_picarro_plot(None, ({'Summit CH4': [dates, ch4]}),
                                limits={'right': date_limits.get('right', None),
                                        'left': date_limits.get('left', None),
                                        'bottom': 1800,
                                        'top': 2200},
                                major_ticks=major_ticks,
                                minor_ticks=minor_ticks)

            ch4_plot = Plot(plotdir / name, True)  # stage plots to be uploaded
            core_session.add(ch4_plot)

        logger.info('New data plots were created.')

        session.close()
        engine.dispose()

        core_session.commit()
        core_session.close()
        core_engine.dispose()
        return True
    except Exception as e:
        logger.error(f'Exception {e.args} occurred in plot_new_data()')
        send_processor_email(PROC, exception=e)

        session.close()
        engine.dispose()

        core_session.close()
        core_engine.dispose()
        return False


async def main():
    try:
        from summit_core import picarro_dir as rundir
        from summit_core import configure_logger
        logger = configure_logger(rundir, __name__)
    except Exception as e:
        print(f'Error {e.args} prevented logger configuration.')
        send_processor_email(PROC, exception=e)
        return

    try:
        if await asyncio.create_task(check_load_new_data(logger)):

            await asyncio.create_task(plot_new_data(logger))

            if await asyncio.create_task(find_cal_events(logger)):
                await asyncio.create_task(create_mastercals(logger))

        return True
    except Exception as e:
        logger.error(f'Exception {e.args} occurred in Picarro main()')
        send_processor_email(PROC, exception=e)
        return False


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
