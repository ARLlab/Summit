import os
import asyncio
from summit_errors import send_processor_email

PROC = 'VOC Processor'


async def check_load_logs(logger):
    """
    Checks the directory against the database for new log files. Loads and commits
    to db if there are any new files.

    Basic format: Connect to the db, check for new log files. If new file
    exists, load and commit them to the db. In all cases, sleep for (n) seconds before
    looping back.
    """

    try:
        import os
        from summit_core import voc_logs_path as logpath
        from summit_core import voc_dir as rundir
        from summit_core import connect_to_db, TempDir
        from summit_voc import LogFile, read_log_file, Base

    except ImportError as e:
        logger.error('Import in check_load_logs() failed.')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_voc.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error('Connection to VOC database failed in check_load_logs()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running check_load_logs()')
        log_files = session.query(LogFile).order_by(LogFile.id).all()  # list of all present log objects

        logfns = [file.name for file in os.scandir(logpath) if 'l.txt' in file.name]

        if logfns:
            logs_in_db = [log.filename for log in log_files]  # log names

            logs_to_load = []
            for log in logfns:
                if log not in logs_in_db:
                    logs_to_load.append(log)  # add files if not in the database filenames

            if logs_to_load:
                new_logs = []
                with TempDir(logpath):
                    for log in logs_to_load:
                        new_logs.append(read_log_file(log))

                if new_logs:
                    for item in new_logs:
                        session.merge(item)
                        logger.info(f'Log File {item} added.')
                    session.commit()

                    session.close()
                    engine.dispose()

                    return True
            else:
                logger.info('No new logs were loaded.')
                return False
        else:
            logger.critical('No log files found in directory.')
            return False

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in check_load_logs().')
        send_processor_email(PROC, exception=e)
        return False


async def check_load_pas(logger):
    '''
    Basic format: Checks the file size of the PA log, opens it if it's bigger
    than before, and reads from the last recorded line onwards. Any new lines
    are added as objects and committed. All exits sleep for (n) seconds before re-upping.

    '''

    pa_file_size = 0  # always assume all lines could be new when initialized
    start_line = 0
    # TODO : These should be DB-stored in a /core db

    try:
        logger.info('Running check_load_pas()')
        from summit_core import voc_LOG_path as pa_path
        from summit_core import voc_dir as rundir
        from summit_core import connect_to_db, TempDir, check_filesize
        from summit_voc import Base, NmhcLine, read_pa_line, name_summit_peaks
    except ImportError as e:
        logger.error('Imports failed in check_load_logs()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_voc.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Error {e.args} connecting to database in check_load_pas()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        nmhc_lines = session.query(NmhcLine).order_by(NmhcLine.id).all()
        line_dates = [line.date for line in nmhc_lines]


        if pa_path.is_file():
            with TempDir(rundir):
                new_file_size = check_filesize(pa_path)

            if new_file_size > pa_file_size:
                with TempDir(rundir):
                    contents = pa_path.read_text().split('\n')

                contents[:] = [c for c in contents if c]  # keep only lines with information

                new_lines = []
                for line in contents[start_line:]:
                    try:
                        with TempDir(rundir):
                            new_lines.append(read_pa_line(line))
                    except:
                        logger.warning('A line in NMHC_PA.LOG was not processed by read_pa_line() due to an exception.')
                        logger.warning(f'That line was: {line}')

                if not len(new_lines):
                    logger.info('No new pa lines were added.')
                    return False

                else:
                    # If list isn't empty, attempt to name all peaks
                    new_lines[:] = [name_summit_peaks(line) for line in new_lines]

                ct = 0
                for item in new_lines:
                    if item.date not in line_dates:  # prevents duplicates in db
                        line_dates.append(item.date)  # prevents duplicates in one load
                        session.merge(item)
                        logger.info(f'PA Line {item} added.')
                        ct += 1

                if ct:
                    session.commit()
                else:
                    logger.info('No new pa lines were added.')
                    return False

                start_line = len(contents)
                pa_file_size = new_file_size  # set filesize to current file size
                return True

            else:
                logger.info('PA file was not larger, so  it was not touched.')
                return False

        else:
            logger.critical('VOC.LOG does not exist.')
            return False
    except Exception as e:
        logger.error(f'Exception {e.args} occurred in check_load_pas()')
        send_processor_email(PROC, exception=e)
        return False


async def load_crfs(logger):

    try:
        from summit_core import voc_dir as rundir
        from summit_core import connect_to_db, TempDir
        from summit_voc import Base, Crf, read_crf_data
    except ImportError as e:
        logger.error('ImportError occurred in load_crfs()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_voc.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Exception {e.args} prevented connection to the database in load_crfs()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running load_crfs()')
        with TempDir(rundir):
            crfs = read_crf_data('summit_CRFs.txt')

        crfs_in_db = session.query(Crf).order_by(Crf.id).all()
        crf_dates = [rf.date_start for rf in crfs_in_db]

        for rf in crfs:
            if rf.date_start not in crf_dates:  # prevent duplicates in db
                crf_dates.append(rf.date_start)  # prevent duplicates in this load
                session.merge(rf)
                logger.info(f'CRF {rf} added.')

        session.commit()
        return True

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in load_crfs()')
        send_processor_email(PROC, exception=e)
        return False


async def create_gc_runs(logger):
    try:
        from summit_core import voc_dir as rundir
        from summit_core import connect_to_db
        from summit_voc import Base, LogFile, NmhcLine, GcRun
        from summit_voc import match_log_to_pa
    except ImportError as e:
        logger.error('Import error in create_gc_runs()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_voc.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the database in create_gc_runs()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running create_gc_runs()')
        nmhc_lines = (session.query(NmhcLine)
                     .filter(NmhcLine.status == 'single')
                     .order_by(NmhcLine.id).all())

        log_files = (session.query(LogFile)
                    .filter(LogFile.status == 'single')
                    .order_by(LogFile.id).all())

        if not len(log_files) or not len(nmhc_lines):
            logger.info('No new logs or pa lines matched.')
            session.close()
            engine.dispose()
            return False

        gc_runs = session.query(GcRun).order_by(GcRun.id).all()
        run_dates = [run.date_end for run in gc_runs]

        gc_runs = match_log_to_pa(log_files, nmhc_lines)

        if not gc_runs:
            logger.info('No new logs or pa lines matched.')
            session.close()
            engine.dispose()
            return False
        else:
            for run in gc_runs:
                if run.date_end not in run_dates:
                    run_dates.append(run.date_end)
                    session.merge(run)
                    logger.info(f'GC Run {run} added.')

            session.commit()

        session.close()
        engine.dispose()
        return True
    except Exception as e:
        logger.error(f'Error {e.args} occurred in create_gc_runs()')
        send_processor_email(PROC, exception=e)
        return False


async def integrate_runs(logger):
    try:
        from summit_core import voc_dir as rundir
        from summit_core import connect_to_db
        from summit_voc import find_crf
        from summit_voc import Base, GcRun, Datum, Crf
    except ImportError as e:
        logger.error(f'ImportError occurred in integrate_runs()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_voc.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the database in integrate_runs()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running integrate_runs()')
        gc_runs = (session.query(GcRun)
                  .filter(GcRun.data_id == None)
                  .order_by(GcRun.id).all())  # get all un-integrated runs

        crfs = session.query(Crf).order_by(Crf.id).all()  # get all crfs

        data = []  # Match all runs with available CRFs
        for run in gc_runs:
            run.crfs = find_crf(crfs, run.date_end)
            session.commit()  # commit changes to crfs?
            data.append(run.integrate())

        data_in_db = session.query(Datum).order_by(Datum.id).all()
        data_dates = [d.date_end for d in data_in_db]

        if len(data):
            for datum in data:
                if datum is not None and datum.date_end not in data_dates:  # prevent duplicates in db
                    data_dates.append(datum.date_end)  # prevent duplicates on this load
                    session.merge(datum)
                    logger.info(f'Data {datum} was added.')

        else:
            logger.info('No new runs were integrated.')
            return False

        session.commit()

        session.close()
        engine.dispose()
        return True

    except Exception as e:
        logger.error('Exception {e.args} occurred in integrate_runs()')
        send_processor_email(PROC, exception=e)
        session.close()
        engine.dispose()
        return False


async def plot_new_data(logger):
    data_len = 0  # always start with plotting when initialized
    days_to_plot = 7

    try:
        from summit_core import voc_dir as rundir
        from summit_core import core_dir, Plot
        from summit_core import connect_to_db, TempDir, create_daily_ticks
        from summit_voc import Base, summit_voc_plot, get_dates_peak_info
        from datetime import datetime
        import datetime as dt
        plotdir = rundir / '../summit_master/summit_master/static/img/coding'  # local flask static folder
    except ImportError as e:
        logger.error('Import error occurred in plot_new_data()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_voc.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the database in plot_new_data()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        core_engine, core_session = connect_to_db('sqlite:///summit_core.sqlite', core_dir)
        Plot.__table__.create(core_engine)
    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the core database in plot_new_data()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running plot_new_data()')
        date_ago = datetime.now() - dt.timedelta(
            days=days_to_plot + 1)  # set a static for retrieving data at beginning of plot cycle

        date_limits, major_ticks, minor_ticks = create_daily_ticks(days_to_plot)

        try:
            _, dates = get_dates_peak_info(session, 'ethane', 'mr', date_start=date_ago)  # get dates for data length
            assert dates is not None

        except (ValueError, AssertionError):
            logger.error('No new data was found within time window. Plots were not created.')
            session.close()
            engine.dispose()
            return False

        if len(dates) != data_len:

            logger.info('New data found to be plotted.')

            with TempDir(plotdir):  ## PLOT ethane and propane
                ethane_mrs, ethane_dates = get_dates_peak_info(session, 'ethane', 'mr', date_start=date_ago)
                propane_mrs, propane_dates = get_dates_peak_info(session, 'propane', 'mr', date_start=date_ago)
                name = summit_voc_plot(None, ({'Ethane': [ethane_dates, ethane_mrs],
                                        'Propane': [propane_dates, propane_mrs]}),
                                limits={'right': date_limits.get('right', None),
                                        'left': date_limits.get('left', None),
                                        'bottom': 0},
                                major_ticks=major_ticks,
                                minor_ticks=minor_ticks)

                ethane_plot = Plot(plotdir/name, True)
                core_session.add(ethane_plot)

            with TempDir(plotdir):  ## PLOT i-butane, n-butane, acetylene
                ibut_mrs, ibut_dates = get_dates_peak_info(session, 'i-butane', 'mr', date_start=date_ago)
                nbut_mrs, nbut_dates = get_dates_peak_info(session, 'n-butane', 'mr', date_start=date_ago)
                acet_mrs, acet_dates = get_dates_peak_info(session, 'acetylene', 'mr', date_start=date_ago)

                name = summit_voc_plot(None, ({'i-Butane': [ibut_dates, ibut_mrs],
                                        'n-Butane': [nbut_dates, nbut_mrs],
                                        'Acetylene': [acet_dates, acet_mrs]}),
                                limits={'right': date_limits.get('right', None),
                                        'left': date_limits.get('left', None),
                                        'bottom': 0},
                                major_ticks=major_ticks,
                                minor_ticks=minor_ticks)

                c4_plot = Plot(plotdir/name, True)
                core_session.add(c4_plot)

            with TempDir(plotdir):  ## PLOT i-pentane and n-pentane, & ratio
                ipent_mrs, ipent_dates = get_dates_peak_info(session, 'i-pentane', 'mr', date_start=date_ago)
                npent_mrs, npent_dates = get_dates_peak_info(session, 'n-pentane', 'mr', date_start=date_ago)

                inpent_ratio = []

                if ipent_mrs is not None and npent_mrs is not None:
                    for i, n in zip(ipent_mrs, npent_mrs):
                        if not n:
                            inpent_ratio.append(None)
                        elif not i:
                            inpent_ratio.append(None)
                        else:
                            inpent_ratio.append(i / n)

                    name = summit_voc_plot(None, ({'i-Pentane': [ipent_dates, ipent_mrs],
                                            'n-Pentane': [npent_dates, npent_mrs]}),
                                    limits={'right': date_limits.get('right', None),
                                            'left': date_limits.get('left', None),
                                            'bottom': 0},
                                    major_ticks=major_ticks,
                                    minor_ticks=minor_ticks)

                    in_pent_plot = Plot(plotdir/name, True)
                    core_session.add(in_pent_plot)

                    name = summit_voc_plot(None, ({'i/n Pentane ratio': [ipent_dates, inpent_ratio]}),
                                    limits={'right': date_limits.get('right', None),
                                            'left': date_limits.get('left', None),
                                            'bottom': 0,
                                            'top': 3},
                                    major_ticks=major_ticks,
                                    minor_ticks=minor_ticks)

                    in_pent_ratio_plot = Plot(plotdir/name, True)
                    core_session.add(in_pent_ratio_plot)

            with TempDir(plotdir):  ## PLOT benzene and toluene
                benz_mrs, benz_dates = get_dates_peak_info(session, 'benzene', 'mr', date_start=date_ago)
                tol_mrs, tol_dates = get_dates_peak_info(session, 'toluene', 'mr', date_start=date_ago)

                name = summit_voc_plot(None, ({'Benzene': [benz_dates, benz_mrs],
                                        'Toluene': [tol_dates, tol_mrs]}),
                                limits={'right': date_limits.get('right', None),
                                        'left': date_limits.get('left', None),
                                        'bottom': 0},
                                major_ticks=major_ticks,
                                minor_ticks=minor_ticks)

                benz_tol_plot = Plot(plotdir/name, True)
                core_session.add(benz_tol_plot)

            data_len = len(dates)

            logger.info('New data plots created.')

            session.close()
            engine.dispose()

            core_session.commit()
            core_session.close()
            core_engine.dispose()
            return True

        else:
            logger.info('No new data, plots were not created.')

            session.close()
            engine.dispose()

            core_session.close()
            core_engine.dispose()
            return False

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in plot_new_data()')
        send_processor_email(PROC, exception=e)
        return False


async def main():
    try:
        from summit_core import voc_dir as rundir
        from summit_core import configure_logger
        logger = configure_logger(rundir, __name__)
    except Exception as e:
        print(f'Error {e.args} prevented logger configuration.')
        send_processor_email(PROC, exception=e)
        return

    try:
        new_logs = await asyncio.create_task(check_load_logs(logger))
        new_lines = await asyncio.create_task(check_load_pas(logger))

        if new_logs or new_lines:
            await asyncio.create_task(load_crfs(logger))
            if await asyncio.create_task(create_gc_runs(logger)):
                if await asyncio.create_task(integrate_runs(logger)):
                    await asyncio.create_task(plot_new_data(logger))

    except Exception as e:
        logger.critical(f'Exception {e.args} caused a complete failure of the VOC processing.')
        send_processor_email(PROC, exception=e)
        return False

    return True


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
