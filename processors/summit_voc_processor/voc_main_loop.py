import os
import asyncio
from summit_errors import send_processor_email

PROC = 'VOC Processor'

def crf_warning_body(rf):
    return ('Multiple exact matches found for a CRF between'
            + f'{rf.date_start} and {rf.date_end}.\n'
            + 'The CRF needs to be edited with any overlapping'
            + ' start and end dates removed.')


async def check_load_logs(logger):
    """
    Check for new logfiles and convert new files to LogFile objects for persistence.

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

    try:
        import os
        import math
        from summit_core import voc_logs_path as logpath
        from summit_core import voc_dir as rundir
        from summit_core import connect_to_db, TempDir, core_dir, Config
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

        logfns = [file.name for file in os.scandir(logpath) if 'l.txt' in file.name]
        if logfns:
            # if greater than 999 files, SQL throws error, use list comprehension to query in chunks
            if len(logfns) >= 999:
                logger.info('Number of Logs > 999, processing in chunks')
                # number of files to query in a given chunk
                iterations = math.ceil(len(logfns) / 1000)
                n = int(len(logfns) / (iterations * 2))

                # list comprehension to seperate logfns into seperate chunks
                chunked_logfiles = [logfns[i * n:(i + 1) * n] for i in range((len(logfns) + n - 1) // n)]

                # query each chunk and append it to the total logs in db
                logs_in_db = []
                for i in range(len(chunked_logfiles)):
                    database_logs = session.query(LogFile).filter(LogFile.filename.in_(chunked_logfiles[i])).all()
                    logs_in_db = logs_in_db + database_logs

                logs_in_db[:] = [l.filename for l in logs_in_db]
            # else, the number of logfiles is normal and under 1000
            else:
                logs_in_db = session.query(LogFile).filter(LogFile.filename.in_(logfns)).all()
                logs_in_db[:] = [l.filename for l in logs_in_db]

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
    """
    Check for new lines in the PA log. Convert them to NmhcLine objects for persistence if they're new.

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

    try:
        logger.info('Running check_load_pas()')
        from summit_core import voc_LOG_path as pa_path
        from summit_core import voc_dir as rundir
        from summit_core import connect_to_db, TempDir, check_filesize, core_dir, Config
        from summit_voc import Base, NmhcLine, read_pa_line, name_summit_peaks, CompoundWindow
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
        core_engine, core_session = connect_to_db('sqlite:///summit_core.sqlite', core_dir)
        Config.__table__.create(core_engine, checkfirst=True)

        voc_config = core_session.query(Config).filter(Config.processor == PROC).one_or_none()

        if not voc_config:
            voc_config = Config(processor=PROC)  # use all default values except processor on init
            core_session.add(voc_config)
            core_session.commit()

    except Exception as e:
        logger.error(f'Error {e.args} prevented configuring the core database in check_load_logs()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        if pa_path.is_file():
            with TempDir(rundir):
                new_file_size = check_filesize(pa_path)

            if new_file_size > voc_config.filesize:

                with TempDir(rundir):
                    contents = pa_path.read_text().split('\n')

                new_startline = len(contents) - 4  # only read what's necessary, but w/ some padding

                contents[:] = [c for c in contents if c]  # keep only lines with information

                new_lines = []
                for line in contents[voc_config.pa_startline:]:
                    try:
                        with TempDir(rundir):
                            new_lines.append(read_pa_line(line))
                    except:
                        logger.warning('A line in NMHC_PA.LOG was not processed by read_pa_line() due to an exception.')
                        logger.warning(f'That line was: {line}')

                if not new_lines:
                    logger.info('No new pa lines were added.')
                    return False

                else:
                    # If list isn't empty, attempt to name all peaks
                    run_one = 1
                    rt_windows = None

                    for ind, line in enumerate(new_lines):

                        if run_one or not (rt_windows.date_start < line.date < rt_windows.date_end):
                            # get rt_windows on first iteration OR retrieve if the current sample is outside the past
                            # window. This should prevent constant needless queries
                            rt_windows = (session.query(CompoundWindow)
                                          .filter(
                                (CompoundWindow.date_start < line.date) & (CompoundWindow.date_end > line.date))
                                          .one_or_none())
                            if run_one:
                                run_one = 0

                        if not rt_windows:
                            logger.warning(f'No retention time windows found for NmhcLine for {line.date}.'
                                           + 'It was not quantified.')
                            run_one = 1  # reset so next sample will re-check for a Window
                            continue

                        new_lines[ind] = name_summit_peaks(line, rt_windows)

                ct = 0
                if new_lines:
                    line_dates = [line.date for line in new_lines]
                    lines_in_db = session.query(NmhcLine.date).filter(NmhcLine.date.in_(line_dates)).all()
                    lines_in_db[:] = [l.date for l in lines_in_db]

                    for item in new_lines:
                        if item.date not in lines_in_db:  # prevents duplicates in db
                            lines_in_db.append(item.date)  # prevents duplicates in one load
                            session.merge(item)
                            logger.info(f'PA Line {item} added.')
                            ct += 1

                if ct:
                    voc_config.pa_startline = new_startline
                    core_session.merge(voc_config)

                    voc_config.filesize = new_file_size
                    core_session.merge(voc_config)

                    core_session.commit()
                    session.commit()
                else:
                    logger.info('No new pa lines were added.')
                    return False

                session.close()
                core_session.close()
                engine.dispose()
                core_engine.dispose()

                return True

            else:
                session.close()
                core_session.close()
                engine.dispose()
                core_engine.dispose()
                logger.info('PA file was not larger, so  it was not touched.')
                return False

        else:
            session.close()
            core_session.close()
            engine.dispose()
            core_engine.dispose()
            logger.critical('VOC.LOG does not exist.')
            return False

    except Exception as e:
        session.close()
        core_session.close()
        engine.dispose()
        core_engine.dispose()
        logger.error(f'Exception {e.args} occurred in check_load_pas()')
        send_processor_email(PROC, exception=e)
        return False


async def load_crfs(logger):
    """
    Read the CRF file and commit any new Crf objects to the database.

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error, False if not
    """

    try:
        from summit_core import voc_dir as rundir
        from summit_core import connect_to_db, TempDir
        from summit_voc import Base, Crf, read_crf_data
        from sqlalchemy import or_, and_
        from summit_errors import send_processor_warning
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
            crfs = read_crf_data('summit_CRFs.txt')  # creates a list of Crf objects

        rfs_to_process = []
        for rf in crfs:
            exact_match = (session.query(Crf)
                                  .filter(Crf.date_start == rf.date_start, Crf.date_end == rf.date_end)
                                  .all())

            if exact_match:
                if len(exact_match) > 1:
                    logger.error(f'Multiple exact matches found for a CRF between {rf.date_start} and {rf.date_end}.')
                    send_processor_warning('CRF', 'Loading', crf_warning_body(rf))
                exact_match = exact_match[0]

                # update the date_end and compound dict if only date_start matches, don't process the one from the file
                exact_match.compounds = rf.compounds
                exact_match.date_revision = rf.date_revision
                session.merge(exact_match)
                logger.info(f'CRF for {exact_match.date_start} to {exact_match.date_end} updated (exact).')

            else:
                # if no matching start and end dates, look for just start date
                start_match = (session.query(Crf)
                               .filter(Crf.date_start == rf.date_start)
                               .all())
                if start_match:
                    if len(start_match) > 1:
                        logger.error(
                            f'Multiple start matches found for a CRF between {rf.date_start} and {rf.date_end}.')
                        send_processor_warning('CRF', 'Loading', crf_warning_body(rf))
                    start_match = start_match[0]
                    # update the date_end and compound dict if only date_start matches
                    start_match.date_end = rf.date_end
                    start_match.compounds = rf.compounds
                    start_match.date_revision = rf.date_revision
                    session.merge(start_match)
                    logger.info(f'CRF for {start_match.date_start} to {start_match.date_end} updated (start).')

                else:
                    # If there's no exact_match or start_match, we want to add this CRF as new, but, first, we want
                    # to make sure it doesn't overlap with any CRFs already in the database, which could happen with
                    # large re-workings of the CRF file.

                    conditions = [
                                  and_(Crf.date_start < rf.date_start, Crf.date_end > rf.date_start),
                                  # ^ date_start of rf to add is within limits of one in the database
                                  and_(Crf.date_start < rf.date_end, Crf.date_end > rf.date_end),
                                  # ^ date_end of rf to add is within limits of one in the database
                                  and_(Crf.date_start > rf.date_start, Crf.date_start < rf.date_end),
                                  # ^ one in database has a date start contained in the bounds of the one to add
                                  and_(Crf.date_end > rf.date_start, Crf.date_end < rf.date_end)
                                  # ^ one in database has a date end contained in the bounds of the one to add
                                  ]

                    overlapping = (session.query(Crf)
                                          .filter(or_(*conditions))
                                          .all())

                    if overlapping:
                        # delete any overlapping CRFs in the database before adding this one
                        for over in overlapping:
                            logger.info(f'Deleting CRF from database for {over.date_start} to {over.date_end}')
                            logger.info(f'It overlapped with CRF for {rf.date_start} to {rf.date_end} from the file.')
                            session.delete(over)  # delete any in the database if their start/end dates overlap

                    rfs_to_process.append(rf)

        for rf in rfs_to_process:
            session.add(rf)
            logger.info(f'CRF for {rf.date_start} to {rf.date_end} added (new).')

        session.commit()

        session.close()
        engine.dispose()

        return True

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in load_crfs()')
        send_processor_email(PROC, exception=e)
        session.close()
        engine.dispose()
        return False


async def add_compound_windows(logger):
    try:
        logger.info('Running add_compound_windows()')
        from datetime import datetime
        from summit_core import voc_LOG_path as pa_path
        from summit_core import voc_dir as rundir
        from summit_core import connect_to_db, TempDir, check_filesize, core_dir
        from summit_voc import Base, NmhcLine, read_pa_line, name_summit_peaks, CompoundWindow
        from summit_voc import compound_windows_1, compound_windows_2
    except ImportError as e:
        logger.error('Imports failed in add_compound_windows()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_voc.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Error {e.args} connecting to database in add_compound_windows()')
        send_processor_email(PROC, exception=e)
        return False

    try:

        windows_in_db = session.query(CompoundWindow).all()
        dates_in_db = [w.date_start for w in windows_in_db]

        window1 = CompoundWindow(datetime(2018, 11, 1), datetime(2019, 4, 9),
                                 compound_windows_1)  # TODO: This is a guess
        window2 = CompoundWindow(datetime(2019, 4, 9), datetime(2019, 12, 31), compound_windows_2)  # TODO: Also a guess

        if window1.date_start not in dates_in_db:
            session.add(window1)

        if window2.date_start not in dates_in_db:
            session.add(window2)

        session.commit()
        session.close()
        engine.dispose()
        return True

    except Exception as e:
        logger.error(f'Error {e.args} occurred in add_compound_windows()')
        session.commit()
        session.close()
        engine.dispose()
        return False


async def create_gc_runs(logger):
    """
    If there are unmatched NmhcLines or LogFiles, check for matches between them and create GcRun objects
    where applicable. GcRuns are the combination of an NmhcLine and LogFile, and represent a completed run on the GC-FID

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

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

        if not log_files or not nmhc_lines:
            logger.info('No new logs or pa lines matched.')
            session.close()
            engine.dispose()
            return False

        gc_runs = match_log_to_pa(log_files, nmhc_lines)

        if not gc_runs:
            logger.info('No new logs or pa lines matched.')
            session.close()
            engine.dispose()
            return False
        else:
            run_dates = [run.date for run in gc_runs]
            run_dates_in_db = session.query(GcRun.date).filter(GcRun.date.in_(run_dates)).all()
            run_dates_in_db[:] = [run.date for run in run_dates_in_db]

            for run in gc_runs:
                if run.date not in run_dates_in_db:
                    run_dates_in_db.append(run.date)
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
    """
    Load any unquantified GcRuns and use Crfs to calculate mixing ratios for each identified compound in each GcRun.
    Creates Datum objects when a run has been sucessfully integrated.

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

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
                   .filter(GcRun.data_con == None)
                   .order_by(GcRun.id).all())  # get all un-integrated runs

        crfs = session.query(Crf).order_by(Crf.id).all()  # get all crfs

        data = []  # Match all runs with available CRFs
        for run in gc_runs:
            run.crfs = find_crf(crfs, run.date_end)
            session.commit()  # commit changes to crfs?
            data.append(run.integrate())

        data_in_db = session.query(Datum).order_by(Datum.id).all()
        data_dates = [d.date_end for d in data_in_db]

        if data:
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
        logger.error(f'Exception {e.args} occurred in integrate_runs()')
        send_processor_email(PROC, exception=e)
        session.close()
        engine.dispose()
        return False


async def plot_new_data(logger):
    """
    If newer data exists, plot it going back one week from the day of the plotting.

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

    try:
        from summit_core import voc_dir as rundir
        from summit_core import core_dir, Plot, Config
        from summit_core import connect_to_db, TempDir, create_daily_ticks, add_or_ignore_plot
        from summit_voc import Base, GcRun, summit_voc_plot, get_dates_peak_info
        from pathlib import Path
        from datetime import datetime
        import datetime as dt
        plotdir = rundir / 'plots'  # local flask static folder
        remotedir = r'/data/web/htdocs/instaar/groups/arl/summit/plots'

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
        Plot.__table__.create(core_engine, checkfirst=True)
        Config.__table__.create(core_engine, checkfirst=True)

        voc_config = core_session.query(Config).filter(Config.processor == PROC).one_or_none()

        if not voc_config:
            voc_config = Config(processor=PROC)  # use all default values except processor on init
            core_session.add(voc_config)
            core_session.commit()

    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the core database in plot_new_data()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running plot_new_data()')
        date_ago = datetime.now() - dt.timedelta(
            days=voc_config.days_to_plot + 1)  # set a static for retrieving data at beginning of plot cycle

        date_limits, major_ticks, minor_ticks = create_daily_ticks(voc_config.days_to_plot)

        try:
            dates = session.query(GcRun.date).filter(GcRun.date > date_ago).order_by(GcRun.date).all()
            dates[:] = [d.date for d in dates]
            assert dates

        except (ValueError, AssertionError):
            logger.error('No new data was found within time window. Plots were not created.')
            session.close()
            engine.dispose()
            return False

        if dates[-1] > voc_config.last_data_date:

            logger.info('New data found to be plotted.')

            with TempDir(plotdir):  ## PLOT ethane and propane
                ethane_dates, ethane_mrs = get_dates_peak_info(session, 'ethane', 'mr', date_start=date_ago)
                propane_dates, propane_mrs = get_dates_peak_info(session, 'propane', 'mr', date_start=date_ago)

                name = summit_voc_plot(None, ({'Ethane': [ethane_dates, ethane_mrs],
                                               'Propane': [propane_dates, propane_mrs]}),
                                       limits={'right': date_limits.get('right', None),
                                               'left': date_limits.get('left', None),
                                               'bottom': 0},
                                       major_ticks=major_ticks,
                                       minor_ticks=minor_ticks)

                ethane_plot = Plot(plotdir / name, remotedir, True)
                add_or_ignore_plot(ethane_plot, core_session)

            with TempDir(plotdir):  ## PLOT i-butane, n-butane, acetylene
                ibut_dates, ibut_mrs = get_dates_peak_info(session, 'i-butane', 'mr', date_start=date_ago)
                nbut_dates, nbut_mrs = get_dates_peak_info(session, 'n-butane', 'mr', date_start=date_ago)
                acet_dates, acet_mrs = get_dates_peak_info(session, 'acetylene', 'mr', date_start=date_ago)

                name = summit_voc_plot(None, ({'i-Butane': [ibut_dates, ibut_mrs],
                                               'n-Butane': [nbut_dates, nbut_mrs],
                                               'Acetylene': [acet_dates, acet_mrs]}),
                                       limits={'right': date_limits.get('right', None),
                                               'left': date_limits.get('left', None),
                                               'bottom': 0},
                                       major_ticks=major_ticks,
                                       minor_ticks=minor_ticks)

                c4_plot = Plot(plotdir / name, remotedir, True)
                add_or_ignore_plot(c4_plot, core_session)

            with TempDir(plotdir):  ## PLOT i-pentane and n-pentane, & ratio
                from summit_voc import Peak, LogFile
                from sqlalchemy.orm import aliased
                ipentane = aliased(Peak)
                npentane = aliased(Peak)

                data = (session.query(GcRun.date, ipentane.mr, npentane.mr)
                        .join(ipentane, ipentane.run_id == GcRun.id)
                        .join(npentane, npentane.run_id == GcRun.id)
                        .filter(ipentane.name == 'i-pentane')
                        .filter(npentane.name == 'n-pentane')
                        .filter(GcRun.date >= date_ago)
                        .order_by(GcRun.date)
                        .all())

                pentane_dates = [d.date for d in data]
                ipent_mrs = [d[1] for d in data]
                npent_mrs = [d[2] for d in data]

                inpent_ratio = []

                if ipent_mrs is not None and npent_mrs is not None:
                    for i, n in zip(ipent_mrs, npent_mrs):
                        if not n or not i:
                            inpent_ratio.append(None)
                        else:
                            inpent_ratio.append(i / n)

                    all_mrs = ipent_mrs + npent_mrs
                    all_mrs[:] = [mr for mr in all_mrs if mr]  # remove any 0s or NoneTypes before taking max of list
                    top_plot_limit = max(all_mrs) * 1.05
                    # set the plot max as 5% above the max value

                    name = summit_voc_plot(pentane_dates, ({'i-Pentane': [None, ipent_mrs],
                                                            'n-Pentane': [None, npent_mrs]}),
                                           limits={'right': date_limits.get('right', None),
                                                   'left': date_limits.get('left', None),
                                                   'top': top_plot_limit,
                                                   'bottom': 0},
                                           # 'top': np.amax(ipent_mrs) + .01
                                           major_ticks=major_ticks,
                                           minor_ticks=minor_ticks)

                    in_pent_plot = Plot(plotdir / name, remotedir, True)
                    add_or_ignore_plot(in_pent_plot, core_session)

                    name = summit_voc_plot(pentane_dates, ({'i/n Pentane ratio': [None, inpent_ratio]}),
                                           limits={'right': date_limits.get('right', None),
                                                   'left': date_limits.get('left', None),
                                                   'bottom': 0,
                                                   'top': 3},
                                           major_ticks=major_ticks,
                                           minor_ticks=minor_ticks,
                                           y_label_str='')

                    in_pent_ratio_plot = Plot(plotdir / name, remotedir, True)
                    add_or_ignore_plot(in_pent_ratio_plot, core_session)

            with TempDir(plotdir):  ## PLOT benzene and toluene
                benz_dates, benz_mrs = get_dates_peak_info(session, 'benzene', 'mr', date_start=date_ago)
                tol_dates, tol_mrs = get_dates_peak_info(session, 'toluene', 'mr', date_start=date_ago)

                name = summit_voc_plot(None, ({'Benzene': [benz_dates, benz_mrs],
                                               'Toluene': [tol_dates, tol_mrs]}),
                                       limits={'right': date_limits.get('right', None),
                                               'left': date_limits.get('left', None),
                                               'bottom': 0},
                                       major_ticks=major_ticks,
                                       minor_ticks=minor_ticks)

                benz_tol_plot = Plot(plotdir / name, remotedir, True)
                add_or_ignore_plot(benz_tol_plot, core_session)

            voc_config.last_data_date = dates[-1]
            core_session.merge(voc_config)

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

        session.close()
        engine.dispose()

        core_session.close()
        core_engine.dispose()

        return False


async def plot_logdata(logger):
    """
    Loads dailies for the last 3 weeks and plots with ticks for every three days and minor ticks for every day.
    Plots are registered with the core database so they're uploaded to the Taylor drive.

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

    try:
        import datetime as dt
        from pathlib import Path
        from datetime import datetime
        from summit_core import connect_to_db, TempDir, Config, Plot, add_or_ignore_plot, create_daily_ticks
        from summit_core import voc_dir, core_dir
        from summit_voc import LogFile, summit_log_plot
        from summit_voc import log_params_list as log_parameters
        plotdir = core_dir / 'plots/log'
        remotedir = r'/data/web/htdocs/instaar/groups/arl/summit/protected/plots'

        try:
            os.chdir(plotdir)
        except FileNotFoundError:
            os.mkdir(plotdir)

        try:
            os.chdir(plotdir)
        except FileNotFoundError:
            os.mkdir(plotdir)

    except ImportError as e:
        logger.error(f'ImportError occurred in plot_logdata()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_voc.sqlite', voc_dir)
    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the database in plot_logdata()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        core_engine, core_session = connect_to_db('sqlite:///summit_core.sqlite', core_dir)
        Plot.__table__.create(core_engine, checkfirst=True)
        Config.__table__.create(core_engine, checkfirst=True)

        log_config = core_session.query(Config).filter(Config.processor == 'Log Plotting').one_or_none()

        if not log_config:
            log_config = Config(processor='Log Plotting',
                                days_to_plot=21)  # use all default values except processor on init
            core_session.add(log_config)
            core_session.commit()

    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the core database in plot_logdata()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running plot_logdata()')

        date_ago = datetime.now() - dt.timedelta(
            days=log_config.days_to_plot + 1)  # set a static for retrieving data at beginning of plot cycle

        date_limits, major_ticks, minor_ticks = create_daily_ticks(log_config.days_to_plot, minors_per_day=1)

        major_ticks = [t for ind, t in enumerate(major_ticks) if ind % 3 == 0]  # use every third daily tick

        logs = session.query(LogFile).filter(LogFile.date >= date_ago).order_by(LogFile.date).all()

        logdict = {}
        for param in log_parameters:
            logdict[param] = [getattr(l, param) for l in logs]

        dates = [l.date for l in logs]

        with TempDir(plotdir):  ## PLOT i-butane, n-butane, acetylene

            name = 'trap_starts_ends.png'
            summit_log_plot(name, dates,
                            ({'H20 Trap A, Sample Start': [None, logdict.get('WTA_temp_start')],
                              'H20 Trap B, Sample Start': [None, logdict.get('WTB_temp_start')],
                              'Ads Trap A, Sample Start': [None, logdict.get('adsA_temp_start')],
                              'Ads Trap B, Sample Start': [None, logdict.get('adsB_temp_start')],
                              'H20 Trap A, Sample End': [None, logdict.get('WTA_temp_end')],
                              'H20 Trap B, Sample End': [None, logdict.get('WTB_temp_end')],
                              'Ads Trap A, Sample End': [None, logdict.get('adsA_temp_end')],
                              'Ads Trap B, Sample End': [None, logdict.get('adsB_temp_end')]}),
                            limits={'right': date_limits.get('right', None),
                                    'left': date_limits.get('left', None),
                                    'bottom': -40,
                                    'top': 40},
                            major_ticks=major_ticks,
                            minor_ticks=minor_ticks)

            traps_plot = Plot(plotdir / name, remotedir, True)
            add_or_ignore_plot(traps_plot, core_session)

            name = 'sample_flow_and_pressure.png'
            summit_log_plot(name, dates,
                            ({'Sample Pressure (PSI)': [None, logdict.get('sampleflow2')],
                              'Sample Flow (V)': [None, logdict.get('samplepressure2')]}),
                            limits={'right': date_limits.get('right', None),
                                    'left': date_limits.get('left', None),
                                    'bottom': 0,
                                    'top': 10},
                            y_label_str='',
                            major_ticks=major_ticks,
                            minor_ticks=minor_ticks)

            sample_plot = Plot(plotdir / name, remotedir, True)
            add_or_ignore_plot(sample_plot, core_session)

            name = 'water_trap_hot_temps.png'
            summit_log_plot(name, dates,
                            ({'H20 Trap A Hot Temp': [None, logdict.get('WTA_hottemp')],
                              'H20 Trap B Hot Temp': [None, logdict.get('WTB_hottemp')]}),
                            limits={'right': date_limits.get('right', None),
                                    'left': date_limits.get('left', None),
                                    'bottom': 0,
                                    'top': 90},
                            y_label_str='',
                            major_ticks=major_ticks,
                            minor_ticks=minor_ticks)

            hot_water_plot = Plot(plotdir / name, remotedir, True)
            add_or_ignore_plot(hot_water_plot, core_session)

            name = 'trap_temps_inject_flashheat.png'
            summit_log_plot(name, dates,
                            ({'Trap Temp, Flash Heat': [None, logdict.get('traptempFH')],
                              'Trap Temp, Inject': [None, logdict.get('traptempinject_end')],
                              'Trap Temp, Bakeout': [None, logdict.get('traptempbakeout_end')]}),
                            limits={'right': date_limits.get('right', None),
                                    'left': date_limits.get('left', None),
                                    'bottom': -50,
                                    'top': 350},
                            y_label_str='Pressure (PSI)',
                            major_ticks=major_ticks,
                            minor_ticks=minor_ticks)

            traptemp_plot = Plot(plotdir / name, remotedir, True)
            add_or_ignore_plot(traptemp_plot, core_session)

            name = 'gc_oven_start_end.png'
            summit_log_plot(name, dates,
                            ({'GC Start Temp': [None, logdict.get('GCstarttemp')],
                              'GC Oven Temp': [None, logdict.get('GCoventemp')]}),
                            limits={'right': date_limits.get('right', None),
                                    'left': date_limits.get('left', None),
                                    'bottom': 0,
                                    'top': 300},
                            major_ticks=major_ticks,
                            minor_ticks=minor_ticks)

            ovenstend_plot = Plot(plotdir / name, remotedir, True)
            add_or_ignore_plot(ovenstend_plot, core_session)

            name = 'gc_head_start_end.png'
            summit_log_plot(name, dates,
                            ({'GC Head Pressure Start': [None, logdict.get('GCHeadP')],
                              'GC Head Pressure End': [None, logdict.get('GCHeadP1')]}),
                            limits={'right': date_limits.get('right', None),
                                    'left': date_limits.get('left', None),
                                    'bottom': 0,
                                    'top': 25},
                            major_ticks=major_ticks,
                            minor_ticks=minor_ticks)

            gcheadstend_plot = Plot(plotdir / name, remotedir, True)
            add_or_ignore_plot(gcheadstend_plot, core_session)

        core_session.commit()
        core_session.close()
        core_engine.dispose()

        session.close()
        engine.dispose()
        return True

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in plot_logdata()')
        send_processor_email(PROC, exception=e)
        session.close()
        engine.dispose()

        core_session.close()
        core_engine.dispose()

        return False


async def check_new_logs(logger):
    """
    This function checks new log files to see if each daily parameter is within a specified range. It will loop
    through every parameter of the log file, and send a warning email with the names of parameters that failed for
    any given log file.

    :param logger: logger, for all logging
    :return: Boolean, True if successful, False if an Exception is handled
    """

    try:
        import datetime as dt
        from pathlib import Path
        from datetime import datetime
        from summit_core import connect_to_db, TempDir, Config
        from summit_core import voc_dir, core_dir
        from summit_voc import LogFile, log_parameter_bounds
        from summit_errors import send_logparam_email
        import pandas as pd
    except ImportError as e:
        logger.error(f'ImportError occurred in check_new_logs()')
        send_processor_email('Log Checking', exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_voc.sqlite', voc_dir)
    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the database in check_new_logs()')
        send_processor_email('Log Checking', exception=e)
        return False

    try:
        core_engine, core_session = connect_to_db('sqlite:///summit_core.sqlite', core_dir)
        Config.__table__.create(core_engine, checkfirst=True)

        logcheck_config = core_session.query(Config).filter(Config.processor == 'Log Checking').one_or_none()

        if not logcheck_config:
            logcheck_config = Config(processor='Log Checking')  # use all default values except processor on init
            core_session.add(logcheck_config)
            core_session.commit()

    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the core database in check_new_logs()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running check_new_logs()')

        # Query the VOC Database for the most recent logfile data
        recentDate = (session                                                   # open session
                      .query(LogFile.date)                                      # gather date
                      .order_by(LogFile.date.desc())                            # order by desc
                      .first()[0])                                              # grab just the first value

        # If the most recent date is greater than the last one, we query for all logs greater than it, save the date of
        # the last one, and then apply various actions to them
        if recentDate > logcheck_config.last_data_date:
            logfiles = (session
                        .query(LogFile)                                         # query DB for dates
                        .order_by(LogFile.date)                                 # order by desc
                        .filter(LogFile.date > logcheck_config.last_data_date)  # filter only new ones
                        .all())                                                 # get all of them

            lastDate = logfiles[-1].date                                        # identify last log date

            """
            A Note About Log Parameter Checks:

            The "real" names of water trap and ads trap attributes are WTA_temp_start, adsB_temp_end etc, BUT the 
            acceptable values for each depend on which trap is the active one. The active traps should cool to 
            sub-zero temperatures, and the inactive traps should remain around ambient.

            Because of this, the boundary parameters are listed as WT_primary_temp_start, ads_secondary_temp_end etc.
            The attribute that should be retrieved from each logfile is then constructed by replacing _primary or 
            _secondary in the boundary name with A or B depending on the active water and adsorbent traps. This allows:
            getattr(logfile, log_name) to get the primary or secondary trap information and check that it's within 
            limits. 
            """

            # loop through log parameters and identify files outside of acceptable limits
            for log in logfiles:                                                                # loop over each log

                wt_primary = 'A' if log.WTinuse is 0 else 'B'                                   # identify primary and
                ads_primary = 'A' if log.adsTinuse is 0 else 'B'                                # secondary WT/ads
                wt_secondary = 'A' if wt_primary is 'B' else 'B'                                # depending on used
                ads_secondary = 'A' if ads_primary is 'B' else 'B'

                failed = []                                                                     # prealloc failed list

                for name, limits in log_parameter_bounds.items():                                        # loop over param dict

                    # replace primary and secondary with appropriate classification (A or B)
                    if '_primary' in name or '_secondary' in name:
                        if 'WT_' in name:
                            log_name = name.replace('_primary', wt_primary).replace('_secondary', wt_secondary)
                        elif 'ads_' in name:
                            log_name = name.replace('_primary', ads_primary).replace('_secondary', ads_secondary)
                        else:
                            logger.error("""THIS CAN'T BE HAPPENING. 
                                            A parameter in the logs contains _primary or _secondary, but does not
                                            contain WT_ or ads_""")
                            log_name = None
                    # otherwise use the default parameter name
                    else:
                        log_name = name

                    log_value = getattr(log, log_name)                                           # limit tuple

                    if not limits[0] <= log_value <= limits[1]:                                 # items outside of bound
                        failed.append(name)                                                   # append failed name

                        # print log statement for error identification in addition to email
                        if log_name != name:
                            logger.warning(f'Log {log.filename} failed due to parameter {name}/{log_name}')
                        else:
                            logger.warning(f'Log {log.filename} failed due to parameter {name}')

                if failed:
                    send_logparam_email(log, failed)                                 # send email with failed

            # Update the date of logcheck_config so we don't check same values twice
            logcheck_config.last_data_date = lastDate

        # Merge, commit, close, and dispose of SQL Databases
        core_session.merge(logcheck_config)

        core_session.commit()
        core_session.close()
        core_engine.dispose()

        session.close()
        engine.dispose()

        return True

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in check_new_logs()')
        send_processor_email('Log Checking', exception=e)
        session.close()
        engine.dispose()

        core_session.close()
        core_engine.dispose()

        return False


async def load_excel_corrections(sheet_name, logger):
    """
    Load the datasheet from another drive.
    Create NmhcCorrections if any new corrections are available, then reintegrate.

    :param sheetpath: Path, to the datasheet to be processed
    :param logger: logger, for all logging
    :return: Boolean, True if successful, False if an Exception is handled
    """

    logger.info(f'Running load_excel_corrections() for {sheet_name}')

    try:
        import pandas as pd
        from pathlib import Path
        from summit_voc import Peak, LogFile, NmhcLine, NmhcCorrection, GcRun, Datum, Base
        from summit_voc import check_sheet_cols, correction_from_df_column, find_approximate_rt, sheet_slices
        from summit_core import connect_to_db, search_for_attr_value, data_file_paths
        from summit_core import voc_dir as rundir
    except ImportError as e:
        logger.error('ImportError occurred in load_excel_corrections()')
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_voc.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the database in load_excel_corrections()')
        send_processor_email(PROC, exception=e)
        return False

    sheetpath = data_file_paths.get(sheet_name + '_sheet')

    try:
        data = pd.read_excel(sheetpath, header=None, usecols=check_sheet_cols).dropna(axis=1, how='all')

        data = data.set_index([0])  # set first row of df to the index
        data.index = data.index.str.lower()
        data = data[data.columns[:-1]]  # drop last row of DF (the one with 'END' in it)

        engine, session = connect_to_db('sqlite:///summit_voc.sqlite', rundir)

        logfiles = session.query(LogFile).order_by(LogFile.samplecode)
        nmhc_lines = session.query(NmhcLine).filter(NmhcLine.correction_id == None).order_by(NmhcLine.id)
        gc_runs = session.query(GcRun).order_by(GcRun.id)

        nmhc_corrections = []

        corrections_in_db = session.query(NmhcCorrection).all()
        correction_codes_in_db = [c.samplecode for c in corrections_in_db]

        with session.no_autoflush:
            for col_name in data.columns.tolist():
                col = data.loc[:, col_name]
                nmhc_corrections.append(correction_from_df_column(col, logfiles, nmhc_lines, gc_runs, logger,
                                                                  sheet_name, correction_codes_in_db))

        for correction in nmhc_corrections:
            if correction:
                if correction.samplecode not in correction_codes_in_db:
                    session.add(correction)
                    logger.info(f'Correction for {correction.samplecode} added.')

        session.commit()

        nmhc_corrections = (session.query(NmhcCorrection)
                            .filter(NmhcCorrection.status == 'unapplied')
                            .filter(NmhcCorrection.date != None)
                            .all())
        # re-get all added corrections that haven't been applied, but have a date from a matched line

        for correction in nmhc_corrections:
            if correction:
                line = session.query(NmhcLine).filter(NmhcLine.correction_id == correction.id).one_or_none()

                if not line:
                    logger.info(f'A matching line for NmhcCorrection {correction} was not found.')
                    continue
            else:
                continue

            for peak_corr in correction.peaklist:
                if not peak_corr.rt:
                    continue

                peak_by_name = search_for_attr_value(line.peaklist, 'name', peak_corr.name)
                peak_by_rt = search_for_attr_value(line.peaklist, 'rt', peak_corr.rt)
                # try to find peak by name, then retention time exact match

                if not peak_by_rt:
                    peak_by_rt = find_approximate_rt(line.peaklist, peak_corr.rt)
                # if peak not found by rt exactly, search with a fuzzy limit

                if (peak_by_name and peak_by_rt) and (peak_by_name is peak_by_rt):  # if they're not None, and identical
                    peak = peak_by_name

                else:
                    if peak_by_name and peak_by_rt:  # if both exist, but not identical, prefer the RT-found one
                        peak_by_name.name = '-'
                        peak_by_rt.name = peak_corr.name
                        peak = peak_by_rt
                        session.merge(peak)
                        session.merge(peak_by_name)

                    elif peak_by_name:  # if only found by name, use the named one
                        peak = peak_by_name
                        session.merge(peak)

                    elif peak_by_rt:  # if only found by rt, use the rt one
                        peak = peak_by_rt
                        peak.name = peak_corr.name
                        session.merge(peak)

                    else:
                        line.peaklist.append(peak_corr)  # if not found at all, add the corrected peak as a new peak
                        logger.warning(f'Peak with name {peak_corr} added to NmhcLine for {line.date}.')

                        continue

                peak.pa = peak_corr.pa
                peak.rt = peak_corr.rt
                peak.rev = peak.rev + 1  # Sqlite *does not* like using += notation

            correction.status = 'applied'

            line.nmhc_corr_con = correction
            correction.correction_id = line

            data = session.query(Datum).filter(Datum.line_id == line.id).one_or_none()

            if data:
                data.reintegrate()
                session.merge(data)
            # find and reintegrate data after correcting peaks

            session.merge(correction)
            session.merge(line)
            logger.info(f'Successful peak corrections made to {line.date}')

        session.commit()
        session.close()
        engine.dispose()

        return True

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in load_excel_corrections()')
        send_processor_email(PROC, exception=e)
        session.close()
        engine.dispose()
        return False


async def main():
    """
    Run the individual processes in order, only proceeding if they return successfully with new data and warrant
    continuing to the next process.

    :return: Boolean, True if it ran without error and created data, False if not
    """
    try:
        from datetime import datetime
        from summit_core import voc_dir as rundir
        from summit_core import configure_logger
        logger = configure_logger(rundir, __name__)
    except Exception as e:
        print(f'Error {e.args} prevented logger configuration.')
        send_processor_email(PROC, exception=e)
        return

    try:
        await asyncio.create_task(add_compound_windows(logger))
        new_logs = await asyncio.create_task(check_load_logs(logger))
        new_lines = await asyncio.create_task(check_load_pas(logger))

        if new_logs:
            await asyncio.create_task(plot_logdata(logger))
            await asyncio.create_task(check_new_logs(logger))

        if new_logs or new_lines:
            await asyncio.create_task(load_crfs(logger))
            if await asyncio.create_task(create_gc_runs(logger)):
                await asyncio.create_task(integrate_runs(logger))

        if datetime.now().hour < 1 and datetime.now().minute > 30:  # run only between 12:30 and 1AM local
            from summit_voc import sheet_slices
            for sheet_name in sheet_slices.keys():
                await asyncio.create_task(load_excel_corrections(sheet_name, logger))

        await asyncio.create_task(plot_new_data(logger))

        return True

    except Exception as e:
        logger.critical(f'Exception {e.args} caused a complete failure of the VOC processing.')
        send_processor_email(PROC, exception=e)
        return False


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
