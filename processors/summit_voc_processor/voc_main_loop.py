import os
import asyncio
from summit_errors import send_processor_email

PROC = 'VOC Processor'


async def check_load_logs(logger):
    """
    Check for new logfiles and convert new files to LogFile objects for persistence.

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

    try:
        import os
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
        nmhc_lines = session.query(NmhcLine).order_by(NmhcLine.id).all()
        line_dates = [line.date for line in nmhc_lines]

        if pa_path.is_file():
            with TempDir(rundir):
                new_file_size = check_filesize(pa_path)

            if new_file_size > voc_config.filesize:
                voc_config.filesize = new_file_size
                core_session.merge(voc_config)
                core_session.commit()

                with TempDir(rundir):
                    contents = pa_path.read_text().split('\n')

                new_startline = len(contents)  # get length of file before any lines omitted

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
                                          .filter((CompoundWindow.date_start < line.date) & (CompoundWindow.date_end > line.date))
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
                for item in new_lines:
                    if item.date not in line_dates:  # prevents duplicates in db
                        line_dates.append(item.date)  # prevents duplicates in one load
                        session.merge(item)
                        logger.info(f'PA Line {item} added.')
                        ct += 1

                if ct:
                    voc_config.pa_startline = new_startline
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

        window1 = CompoundWindow(datetime(2018,11,1), datetime(2019,4,9), compound_windows_1)  # TODO: This is a guess
        window2 = CompoundWindow(datetime(2019,4,9), datetime(2019,12,31), compound_windows_2) # TODO: Also a guess

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
            _, dates = get_dates_peak_info(session, 'ethane', 'mr', date_start=date_ago)  # get dates for data length
            assert dates is not None

        except (ValueError, AssertionError):
            logger.error('No new data was found within time window. Plots were not created.')
            session.close()
            engine.dispose()
            return False

        if dates[-1] > voc_config.last_data_date:

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
                from summit_voc import Peak, LogFile
                from sqlalchemy.orm import aliased
                ipentane = aliased(Peak)
                npentane = aliased(Peak)

                data = (session.query(LogFile.date, ipentane.mr, npentane.mr)
                        .join(ipentane, ipentane.log_id == LogFile.id)
                        .join(npentane, npentane.log_id == LogFile.id)
                        .filter(ipentane.name == 'i-pentane')
                        .filter(npentane.name == 'n-pentane')
                        .order_by(LogFile.date)
                        .all())

                pentane_dates = [d.date for d in data]
                ipent_mrs = [d[1] for d in data]
                npent_mrs =[d[2] for d in data]

                inpent_ratio = []

                if ipent_mrs is not None and npent_mrs is not None:
                    for i, n in zip(ipent_mrs, npent_mrs):
                        if not n:
                            inpent_ratio.append(None)
                        elif not i:
                            inpent_ratio.append(None)
                        else:
                            inpent_ratio.append(i / n)

                    name = summit_voc_plot(pentane_dates, ({'i-Pentane': [None, ipent_mrs],
                                            'n-Pentane': [None, npent_mrs]}),
                                    limits={'right': date_limits.get('right', None),
                                            'left': date_limits.get('left', None),
                                            'bottom': 0},
                                    major_ticks=major_ticks,
                                    minor_ticks=minor_ticks)

                    in_pent_plot = Plot(plotdir/name, True)
                    core_session.add(in_pent_plot)

                    name = summit_voc_plot(pentane_dates, ({'i/n Pentane ratio': [None, inpent_ratio]}),
                                    limits={'right': date_limits.get('right', None),
                                            'left': date_limits.get('left', None),
                                            'bottom': 0,
                                            'top': 3},
                                    major_ticks=major_ticks,
                                    minor_ticks=minor_ticks,
                                    y_label_str='')

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
        return False


async def load_excel_corrections(sheetpath, logger):

    logger.info('Running load_excel_corrections()')

    try:
        import pandas as pd
        from pathlib import Path
        from summit_voc import Peak, LogFile, NmhcLine, NmhcCorrection, GcRun, Datum, Base
        from summit_voc import check_ambient_sheet_cols, correction_from_df_column, find_approximate_rt
        from summit_core import connect_to_db, search_for_attr_value
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

    try:
        data = pd.read_excel(sheetpath, header=None, usecols=check_ambient_sheet_cols).dropna(axis=1, how='all')

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
                nmhc_corrections.append(correction_from_df_column(col, logfiles, nmhc_lines, gc_runs, logger))

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
        # re-get all added corrections that haven't been applied

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

                if not peak_by_rt:
                    peak_by_rt = find_approximate_rt(line.peaklist, peak_corr.rt)

                if (peak_by_name and peak_by_rt) and (peak_by_name is peak_by_rt):  # if they're not None, and identical
                    peak = peak_by_name

                else:
                    if peak_by_name and peak_by_rt:  # if both exist, but not identical, prefer the RT-found one
                        peak_by_name.name = '-'
                        peak_by_rt.name = peak_corr.name
                        peak = peak_by_rt
                        session.merge(peak)
                        session.merge(peak_by_name)

                    elif peak_by_name:
                        peak = peak_by_name
                        session.merge(peak)

                    elif peak_by_rt:
                        peak = peak_by_rt
                        peak.name = peak_corr.name
                        session.merge(peak)

                    else:
                        line.peaklist.append(peak_corr)
                        logger.warning (f'Peak with name {peak_corr} added to NmhcLine for {line.date}.')

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
        from summit_core import voc_dir as rundir
        from summit_core import configure_logger, ambient_sheet
        logger = configure_logger(rundir, __name__)
    except Exception as e:
        print(f'Error {e.args} prevented logger configuration.')
        send_processor_email(PROC, exception=e)
        return

    try:
        await asyncio.create_task(add_compound_windows(logger))
        new_logs = await asyncio.create_task(check_load_logs(logger))
        new_lines = await asyncio.create_task(check_load_pas(logger))

        if new_logs or new_lines:
            await asyncio.create_task(load_crfs(logger))
            if await asyncio.create_task(create_gc_runs(logger)):
                if await asyncio.create_task(integrate_runs(logger)):
                    await asyncio.create_task(load_excel_corrections(ambient_sheet, logger))
                    await asyncio.create_task(plot_new_data(logger))

        return True

    except Exception as e:
        logger.critical(f'Exception {e.args} caused a complete failure of the VOC processing.')
        send_processor_email(PROC, exception=e)
        return False


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
