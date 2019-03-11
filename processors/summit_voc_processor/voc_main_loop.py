from summit_voc import logger
from pathlib import Path
import os, asyncio

homedir = Path(os.getcwd())
locallogdir = homedir / 'logs'  # folder containing log files
plotdir = homedir / '../summit_master/summit_master/static/img/coding'  # local flask static folder

os.chdir(homedir)


async def check_load_logs(logpath, homedir, sleeptime):
    '''
    Checks the directory against the database for new log files. Loads and commits
    to db if there are any new files.

    Basic format: Connect to the db, check for new log files. If new file
    exists, load and commit them to the db. In all cases, sleep for (n) seconds before
    looping back.
    '''

    while True:
        import os
        from summit_voc import connect_to_summit_db, TempDir, LogFile, read_log_file

        engine, session, Base = connect_to_summit_db('sqlite:///summit_vocs.sqlite', homedir)
        Base.metadata.create_all(engine)

        LogFiles = session.query(LogFile).order_by(LogFile.id).all()  # list of all present log objects

        logfns = [file.name for file in os.scandir(logpath) if 'l.txt' in file.name]

        if len(logfns) is 0:
            await asyncio.sleep(sleeptime)
            logger.warning('There were no log files in the directory.')
            continue # no logs in directory? Sleep and look again

        logs_in_db = [log.filename for log in LogFiles] #log names

        logs_to_load = []
        for log in logfns:
            if log not in logs_in_db:
                logs_to_load.append(log)  # add files if not in the database filenames

        if len(logs_to_load) is 0:
            logger.info('No new logs were found.')
            session.commit()
            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)
        else:
            new_logs = []
            with TempDir(logpath):
                for log in logs_to_load:
                    new_logs.append(read_log_file(log))

            if len(new_logs) != 0:
                for item in new_logs:
                    session.merge(item)
                    logger.info(f'Log File {item} added.')


            session.commit()
            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)


async def check_load_pas(filename, directory, sleeptime):
    '''
    Basic format: Checks the file size of the PA log, opens it if it's bigger
    than before, and reads from the last recorded line onwards. Any new lines
    are added as objects and committed. All exits sleep for (n) seconds before re-upping.
    '''
    pa_file_size = 0 #always assume all lines could be new when initialized
    start_line = 0

    while True:
        logger.info('Running check_load_pas()')
        from summit_voc import connect_to_summit_db, TempDir, NmhcLine, read_pa_line
        from summit_voc import name_summit_peaks

        engine, session, Base = connect_to_summit_db('sqlite:///summit_vocs.sqlite', directory)
        Base.metadata.create_all(engine)

        NmhcLines = session.query(NmhcLine).order_by(NmhcLine.id).all()
        line_dates = [line.date for line in NmhcLines]

        from pathlib import Path

        pa_path = Path(directory)/filename

        if os.path.isfile(pa_path):
            with TempDir(directory):
                new_file_size = os.path.getsize(filename)

            if new_file_size > pa_file_size:
                with TempDir(directory):
                    contents = open(filename).readlines()

                new_lines = []
                for line in contents[start_line:]:
                    try:
                        with TempDir(directory):
                            new_lines.append(read_pa_line(line))
                    except:
                        logger.warning('A line in NMHC_PA.LOG was not processed by read_pa_line() due to an exception.')
                        logger.warning(f'That line was: {line}')

                if len(new_lines) is 0:
                    logger.info('No new pa lines were added.')
                    await asyncio.sleep(sleeptime)
                    continue

                else:
                    # If list isn't empty, attempt to name all peaks
                    new_lines[:] = [name_summit_peaks(line) for line in new_lines]

                for item in new_lines:
                    if item.date not in line_dates: #prevents duplicates in db
                        line_dates.append(item.date) #prevents duplicates in one load
                        session.merge(item)
                        logger.info(f'PA Line {item} added.')

                session.commit()

                start_line = len(contents)
                pa_file_size = new_file_size # set filesize to current file size
                await asyncio.sleep(sleeptime)

            else:
                logger.info('PA file was not larger, so  it was not touched.')
                await asyncio.sleep(sleeptime)

        else:
            print(logger.critical('VOC.LOG does not exist.'))
            await asyncio.sleep(sleeptime)

        await asyncio.sleep(sleeptime)
        session.close()
        engine.dispose()


async def load_crfs(directory, sleeptime):

    while True:
        logger.info('Running load_crfs()')
        from summit_voc import read_crf_data
        from summit_voc import Peak, LogFile, NmhcLine, GcRun, Crf
        from summit_voc import connect_to_summit_db, TempDir

        engine, session, Base = connect_to_summit_db('sqlite:///summit_vocs.sqlite', directory)
        Base.metadata.create_all(engine)

        with TempDir(homedir):
            Crfs = read_crf_data('summit_CRFs.txt')

        Crfs_in_db = session.query(Crf).order_by(Crf.id).all()
        crf_dates = [rf.date_start for rf in Crfs_in_db]

        for rf in Crfs:
            if rf.date_start not in crf_dates: # prevent duplicates in db
                crf_dates.append(rf.date_start) # prevent duplicates in this load
                session.merge(rf)
                logger.info(f'CRF {rf} added.')

        session.commit()

        session.close()
        engine.dispose()
        await asyncio.sleep(sleeptime)


async def create_gc_runs(directory, sleeptime):

    while True:
        logger.info('Running create_gc_runs()')
        from summit_voc import LogFile, NmhcLine, GcRun
        from summit_voc import connect_to_summit_db, match_log_to_pa

        engine, session, Base = connect_to_summit_db('sqlite:///summit_vocs.sqlite', directory)
        Base.metadata.create_all(engine)

        NmhcLines = (session.query(NmhcLine)
                    .filter(NmhcLine.status == 'single')
                    .order_by(NmhcLine.id).all())

        LogFiles = (session.query(LogFile)
                    .filter(LogFile.status == 'single')
                    .order_by(LogFile.id).all())

        if len(LogFiles) == 0 or len(NmhcLines) == 0: # wait then continue if no un-matched logs or lines found
            logger.info('No new logs or pa lines matched.')
            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)
            continue

        GcRuns = session.query(GcRun).order_by(GcRun.id).all()
        run_dates = [run.date_end for run in GcRuns]

        GcRuns = match_log_to_pa(LogFiles, NmhcLines)

        for run in GcRuns:
            if run.date_end not in run_dates:
                run_dates.append(run.date_end)
                session.merge(run)
                logger.info(f'GC Run {run} added.')

        session.commit()

        session.close()
        engine.dispose()
        await asyncio.sleep(sleeptime)


async def integrate_runs(directory, sleeptime):

    while True:
        logger.info('Running integrate_runs()')
        from summit_voc import find_crf
        from summit_voc import GcRun, Datum, Crf

        from summit_voc import connect_to_summit_db

        engine, session, Base = connect_to_summit_db('sqlite:///summit_vocs.sqlite', directory)
        Base.metadata.create_all(engine)

        GcRuns = (session.query(GcRun)
                .filter(GcRun.data_id == None)
                .order_by(GcRun.id).all()) # get all un-integrated runs

        Crfs = session.query(Crf).order_by(Crf.id).all() # get all crfs

        data = [] # Match all runs with available CRFs
        for run in GcRuns:
            run.crfs = find_crf(Crfs, run.date_end)
            session.commit() # commit changes to crfs?
            data.append(run.integrate())

        data_in_db = session.query(Datum).order_by(Datum.id).all()
        data_dates = [d.date_end for d in data_in_db]

        if len(data) is 0:
            logger.info('No new runs were integrated.')
            session.commit()
            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)

        else:
            for datum in data:
                if datum is not None and datum.date_end not in data_dates: # prevent duplicates in db
                    data_dates.append(datum.date_end) # prevent duplicates on this load
                    session.merge(datum)
                    logger.info(f'Data {datum} was added.')

            session.commit()
            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)


async def plot_new_data(directory, plotdir, sleeptime):
    data_len = 0  # always start with plotting when initialized

    days_to_plot = 7

    while True:
        logger.info('Running plot_new_data()')

        from summit_voc import connect_to_summit_db, TempDir
        from summit_voc import get_dates_peak_info, summit_voc_plot
        from datetime import datetime
        import datetime as dt

        engine, session, Base = connect_to_summit_db('sqlite:///summit_vocs.sqlite', directory)
        Base.metadata.create_all(engine)

        now = datetime.now()  # save 'now' as the start of making plots
        date_ago = now - dt.timedelta(days=days_to_plot+1)  # set a static for retrieving data at beginning of plot cycle

        date_limits = dict()
        date_limits['right'] = now.replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(days=1)  # end of day
        date_limits['left'] = date_limits['right'] - dt.timedelta(days=days_to_plot)

        major_ticks = [date_limits['right'] - dt.timedelta(days=x) for x in range(0, days_to_plot+1)]
        minor_ticks = [date_limits['right'] - dt.timedelta(hours=x*6) for x in range(0, days_to_plot*4+1)]
        try:
            _ , dates = get_dates_peak_info(session, 'ethane', 'mr', date_start=date_ago)  # get dates for data length

        except ValueError:
            logger.error('No new data was found within time window. Plots were not created.')
            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)
            continue

        if len(dates) != data_len:

            logger.info('New data found to be plotted.')

            with TempDir(plotdir): ## PLOT ethane and propane
                ethane_mrs, ethane_dates = get_dates_peak_info(session, 'ethane', 'mr', date_start=date_ago)
                propane_mrs, propane_dates = get_dates_peak_info(session, 'propane', 'mr', date_start=date_ago)
                summit_voc_plot(None, ({'Ethane': [ethane_dates, ethane_mrs],
                                      'Propane': [propane_dates, propane_mrs]}),
                              limits={'right': date_limits.get('right',None),
                                      'left': date_limits.get('left', None),
                                      'bottom': 0},
                              major_ticks=major_ticks,
                              minor_ticks=minor_ticks)

            with TempDir(plotdir): ## PLOT i-butane, n-butane, acetylene
                ibut_mrs, ibut_dates = get_dates_peak_info(session, 'i-butane', 'mr', date_start=date_ago)
                nbut_mrs, nbut_dates = get_dates_peak_info(session, 'n-butane', 'mr', date_start=date_ago)
                acet_mrs, acet_dates = get_dates_peak_info(session, 'acetylene', 'mr', date_start=date_ago)

                summit_voc_plot(None, ({'i-Butane': [ibut_dates, ibut_mrs],
                                      'n-Butane': [nbut_dates, nbut_mrs],
                                      'Acetylene': [acet_dates, acet_mrs]}),
                              limits={'right': date_limits.get('right',None),
                                      'left': date_limits.get('left', None),
                                      'bottom': 0},
                              major_ticks=major_ticks,
                              minor_ticks=minor_ticks)

            with TempDir(plotdir): ## PLOT i-pentane and n-pentane, & ratio
                ipent_mrs, ipent_dates = get_dates_peak_info(session, 'i-pentane', 'mr', date_start=date_ago)
                npent_mrs, npent_dates = get_dates_peak_info(session, 'n-pentane', 'mr', date_start=date_ago)

                inpent_ratio = []

                if ipent_mrs is not None and npent_mrs is not None:
                    for i, n in zip(ipent_mrs, npent_mrs):
                        if n == 0 or n == None:
                            inpent_ratio.append(None)
                        elif i == None:
                            inpent_ratio.append(None)
                        else:
                            inpent_ratio.append(i/n)

                    summit_voc_plot(None, ({'i-Pentane': [ipent_dates, ipent_mrs],
                                           'n-Pentane': [npent_dates, npent_mrs]}),
                                  limits={'right': date_limits.get('right',None),
                                          'left': date_limits.get('left', None),
                                          'bottom': 0},
                                  major_ticks=major_ticks,
                                  minor_ticks=minor_ticks)

                    summit_voc_plot(None, ({'i/n Pentane ratio': [ipent_dates, inpent_ratio]}),
                                  limits={'right': date_limits.get('right',None),
                                          'left': date_limits.get('left', None),
                                          'bottom': 0,
                                          'top': 3},
                                  major_ticks=major_ticks,
                                  minor_ticks=minor_ticks)

            with TempDir(plotdir): ## PLOT benzene and toluene
                benz_mrs, benz_dates = get_dates_peak_info(session, 'benzene', 'mr', date_start=date_ago)
                tol_mrs, tol_dates = get_dates_peak_info(session, 'toluene', 'mr', date_start=date_ago)

                summit_voc_plot(None, ({'Benzene': [benz_dates, benz_mrs],
                                      'Toluene': [tol_dates, tol_mrs]}),
                              limits={'right': date_limits.get('right',None),
                                      'left': date_limits.get('left', None),
                                      'bottom': 0},
                              major_ticks=major_ticks,
                              minor_ticks=minor_ticks)

            data_len = len(dates)

            logger.info('New data plots created.')

            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)

        else:
            logger.info('No new data, plots were not created.')

            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)


def main():
	loop = asyncio.get_event_loop()

	loop.create_task(check_load_logs(locallogdir, homedir, 1200))
	loop.create_task(check_load_pas('VOC.LOG', homedir, 1200))
	loop.create_task(create_gc_runs(homedir, 1200))
	loop.create_task(load_crfs(homedir, 1200))
	loop.create_task(integrate_runs(homedir, 1200))
	loop.create_task(plot_new_data(homedir, plotdir, 1200))

	loop.run_forever()

if __name__ == '__main__':
	main()



