"""
Methane has only CH4.LOG, and per-run logs. The .LOG file contains date / peaks (name, peak area, retention time)
This just needs to be read in, parsed for new dates/lines, then those lines need to be parsed for peaks, and
sample peaks need to be quantified on the standard peaks.

The per-run logs contain sample pressures, etc; those are associated with pairs of two runs.

Some runtime QC is needed to prevent quantification from failed standard runs, poor integrations, etc.
"""
from summit_errors import send_processor_email
from datetime import datetime
import statistics as s
import asyncio

PROC = 'Methane Processor'


async def check_load_pa_log(logger):
    """
    Read the PA log and create new PaLine objects if possible.

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

    logger.info('Running check_load_pa_log()')

    try:
        from summit_core import methane_LOG_path as pa_filepath
        from summit_core import connect_to_db, check_filesize, core_dir, Config
        from summit_methane import Base, read_pa_line, PaLine
        from summit_core import methane_dir as rundir
        from pathlib import Path
    except ImportError as e:
        logger.error('ImportError occurred in check_load_pa_log()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_methane.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Exception {e.args} prevented connection to the database in check_load_pa_log()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        core_engine, core_session = connect_to_db('sqlite:///summit_core.sqlite', core_dir)
        Config.__table__.create(core_engine, checkfirst=True)

        ch4_config = core_session.query(Config).filter(Config.processor == PROC).one_or_none()

        if not ch4_config:
            ch4_config = Config(processor=PROC)  # use all default values except processor on init
            core_session.add(ch4_config)
            core_session.commit()

    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the core database in plot_new_data()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        lines_in_db = session.query(PaLine).all()
        dates_in_db = [l.date for l in lines_in_db]

        if check_filesize(pa_filepath) <= ch4_config.filesize:
            logger.info('PA file did not change size.')
            return False

        ch4_config.filesize = check_filesize(pa_filepath)
        core_session.merge(ch4_config)
        core_session.commit()

        pa_file_contents = pa_filepath.read_text().split('\n')[ch4_config.pa_startline:]

        ch4_config.pa_startline = ch4_config.pa_startline + len(pa_file_contents)

        pa_file_contents[:] = [line for line in pa_file_contents if line]

        pa_lines = []
        for line in pa_file_contents:
            pa_lines.append(read_pa_line(line))

        if not pa_lines:
            logger.info('No new PaLines found.')
            return False

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

        core_session.merge(ch4_config)
        core_session.commit()

        session.close()
        engine.dispose()
        core_session.close()
        core_engine.dispose()
        return True

    except Exception as e:
        session.close()
        engine.dispose()
        core_session.close()
        core_engine.dispose()
        logger.error(f'Exception {e.args} occurred in check_load_pa_log()')
        send_processor_email(PROC, exception=e)
        return False


async def check_load_run_logs(logger):
    """
    Read new log files and create new GcRun and Sample objects if possible.

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

    try:
        from summit_core import methane_logs_path
        from summit_core import methane_dir as rundir
        from summit_core import get_all_data_files, connect_to_db
        from summit_methane import Base, GcRun, Sample, read_log_file
    except ImportError as e:
        logger.error('ImportError occurred in check_load_run_logs()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_methane.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Exception {e.args} prevented connection to the database in check_load_pa_log()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running check_load_run_logs()')
        runs_in_db = session.query(GcRun).all()
        samples = session.query(Sample)
        sample_count = samples.count()

        run_dates = [r.date for r in runs_in_db]

        files = get_all_data_files(methane_logs_path, '.txt')

        runs = []
        for file in files:
            runs.append(read_log_file(file))

        new_run_count = 0  # count runs added
        for run in runs:
            if run.date not in run_dates:
                session.add(run)
                logger.info(f'GcRun for {run.date} added.')
                new_run_count += 1

        if not new_run_count:
            logger.info('No new GcRuns added.')
        else:
            session.commit()
            new_sample_count = session.query(Sample).count() - sample_count
            logger.info(f'{new_run_count} GcRuns added, containing {new_sample_count} Samples.')

            if new_run_count * 10 != new_sample_count:
                logger.warning('There were not ten Samples per GcRun as expected.')

        session.close()
        engine.dispose()
        return True

    except Exception as e:
        session.close()
        engine.dispose()

        logger.error(f'Exception {e.args} occurred in check_load_pa_log()')
        send_processor_email(PROC, exception=e)
        return False


async def match_runs_to_lines(logger):
    """
    Read new log files and create new GcRun and Sample objects if possible.

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

    try:
        from summit_core import methane_dir as rundir
        from summit_core import connect_to_db
        from summit_methane import GcRun, PaLine, match_lines_to_runs, Base
    except ImportError as e:
        send_processor_email(PROC, exception=e)
        logger.error('ImportError occured in match_runs_to_lines()')
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_methane.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Exception {e.args} prevented connection to the database in check_load_pa_log()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running match_runs_to_peaks()')

        engine, session = connect_to_db('sqlite:///summit_methane.sqlite', rundir)

        unmatched_lines = session.query(PaLine).filter(PaLine.run == None).all()
        unmatched_runs = session.query(GcRun).filter(GcRun.pa_line_id == None).all()

        # married_runs_count = session.query(GcRun).filter(GcRun.status == 'married').count()

        lines, runs, count = match_lines_to_runs(unmatched_lines, unmatched_runs)

        session.commit()

        if count:
            logger.info(f'{count} GcRuns matched with PaLines.')
            return True
        else:
            logger.info('No new GcRun-PaLine pairs matched.')
            return False


    except Exception as e:
        logger.error(f'Exception {e.args} occurred in match_runs_to_lines()')
        send_processor_email(PROC, exception=e)
        return False


async def match_peaks_to_samples(logger):
    """
    All detected peaks in a run are attached to NmhcLines, but are not linked to Samples until they've passed certain
    criteria.

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

    try:
        from summit_core import methane_dir as rundir
        from summit_core import connect_to_db
        from summit_methane import Peak, Sample, GcRun, Base, sample_rts
        from operator import attrgetter
        import datetime as dt
    except ImportError as e:
        logger.error('ImportError occurred in match_peaks_to_samples()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_methane.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Exception {e.args} prevented connection to the database in check_load_pa_log()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_methane.sqlite', rundir)
    except Exception as e:
        logger.error(f'Exception {e.args} prevented connection to the database.')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running match_peaks_to_samples()')

        unmatched_samples = session.query(Sample).filter(Sample.peak_id == None, Sample.run_id != None).all()

        whole_set = list({s.run_id for s in unmatched_samples})
        # SQLite can't take in clauses with > 1000 variables, so chunk to sets of 500
        if len(whole_set) > 500:  # subdivide set
            def split_set(lst, n):
                for i in range(0, len(lst), n):
                    yield (lst[i:i + n])

            sets = split_set(whole_set, 500)
        else:
            sets = [whole_set]

        runs_w_unmatched_samples = []
        for set in sets:
            runs_w_unmatched_samples.extend((session.query(GcRun)
                                            .filter(GcRun.id.in_(set))
                                            .all()))  # create set of runs that require processing

        for run in runs_w_unmatched_samples:
            # loop through runs containing samples that haven't been matched with peaks
            samples = session.query(Sample).filter(Sample.run_id == run.id).all()
            peaks = session.query(Peak).filter(Peak.pa_line_id == run.pa_line_id)

            for sample in samples:
                sn = sample.sample_num
                potential_peaks = peaks.filter(Peak.rt.between(sample_rts[sn][0], sample_rts[sn][1])).all()
                # filter for peaks in this gc run between the expected retention times given in sample_rts

                if len(potential_peaks):
                    # currently, the criteria for "this is the real peak" is "this is the biggest peak"
                    peak = max(potential_peaks, key=attrgetter('pa'))
                    if peak:
                        sample.peak = peak
                        peak.name = 'CH4_' + str(sample.sample_num)
                        sample.date = run.pa_line.date + dt.timedelta(minutes=peak.rt - 1)
                        session.merge(sample)

        session.commit()
        session.close()
        engine.dispose()
        return True

    except Exception as e:
        logger.error(f'Excetion {e.args} occurred in match_peaks_to_samples()')
        send_processor_email(PROC, exception=e)
        return False


async def add_one_standard(logger):
    """
    Add a single standard (the current working one), so that quantifications are possible. VERY TEMPORARY.

    :param logger: logger, to log events to
    :return: Boolean, True if successful
    """

    try:
        from summit_core import methane_dir as rundir
        from summit_core import connect_to_db
        from summit_methane import Standard, Base
    except ImportError as e:
        logger.error('ImportError occurred in add_one_standard()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_methane.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Exception {e.args} prevented connection to the database in check_load_pa_log()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_methane.sqlite', rundir)

        current_standard_dates = [S.date_st for S in session.query(Standard).all()]

        my_only_standard = Standard('ws_2019', 2067.16, datetime(2019, 1, 1), datetime(2019, 6, 1))

        if my_only_standard.date_st not in current_standard_dates:
            session.merge(my_only_standard)
            session.commit()

        session.close()
        engine.dispose()
        return True

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in add_one_standard()')
        send_processor_email(PROC, exception=e)
        return False


async def quantify_samples(logger):
    """
    On a per-run basis, use std1 to calc samples 1-5 (~3) and std2 to calculate samples 6-10 (~8). Output warnings
    if only one standard in a sample is valid.

    :param logger: logger, to log events to
    :return: Boolean, True if successful
    """

    try:
        from summit_core import methane_dir as rundir
        from summit_core import connect_to_db, search_for_attr_value
        from summit_methane import Standard, GcRun, Base
        from summit_methane import calc_ch4_mr, valid_sample
    except Exception as e:
        logger.error('ImportError occurred in qunatify_samples()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_methane.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Exception {e.args} prevented connection to the database in check_load_pa_log()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running quantify_samples()')

        unquantified_runs = session.query(GcRun).filter(GcRun.median == None).all()

        ct = 0
        for run in unquantified_runs:
            samples = run.samples

            standard = (session.query(Standard)
                        .filter(run.date >= Standard.date_st, run.date < Standard.date_en)
                        .first())  # TODO; Set unique constraints on standards, revert to one_or_none()

            if standard is not None:
                ambients = [sample for sample in samples if (sample.sample_type == 3 and valid_sample(sample))]
                standard1 = search_for_attr_value(samples, 'sample_num', 2)
                standard2 = search_for_attr_value(samples, 'sample_num', 7)

                if not ambients:
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

        if ct:
            logger.info(f'{ct} GcRuns were successfully quantified.')
            session.close()
            engine.dispose()
            return True
        else:
            logger.info('No GcRuns quantified.')
            session.close()
            engine.dispose()
            return False

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in quantify_samples()')
        send_processor_email(PROC, exception=e)
        return False


async def plot_new_data(logger):
    """
    If newer data exists, plot it going back one week from the day of the plotting.

    :param logger: logger, to log events to
    :return: Boolean, True if it ran without error and created data, False if not
    """

    try:
        from summit_core import core_dir, Config
        from summit_core import methane_dir as rundir
        from summit_core import connect_to_db, create_daily_ticks, TempDir, Plot, add_or_ignore_plot
        from summit_methane import Sample, Base, plottable_sample, summit_methane_plot
    except ImportError as e:
        logger.error('ImportError occurred in plot_new_data()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        engine, session = connect_to_db('sqlite:///summit_methane.sqlite', rundir)
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error(f'Exception {e.args} prevented connection to the database in check_load_pa_log()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        core_engine, core_session = connect_to_db('sqlite:///summit_core.sqlite', core_dir)
        Plot.__table__.create(core_engine, checkfirst=True)
        Config.__table__.create(core_engine, checkfirst=True)

        ch4_config = core_session.query(Config).filter(Config.processor == PROC).one_or_none()

        if not ch4_config:
            ch4_config = Config(processor=PROC)  # use all default values except processor on init
            core_session.add(ch4_config)
            core_session.commit()

    except Exception as e:
        logger.error(f'Error {e.args} prevented connecting to the core database in plot_new_data()')
        send_processor_email(PROC, exception=e)
        return False

    try:
        logger.info('Running plot_new_data()')

        engine, session = connect_to_db('sqlite:///summit_methane.sqlite', rundir)

        ambient_samples = (session.query(Sample)
                           # .filter(Sample.date.between(datetime(2019, 1, 1), datetime(2019, 6, 1)))
                           .filter(Sample.sample_type == 3)
                           .order_by(Sample.date)
                           .all())

        ambient_samples[:] = [sample for sample in ambient_samples if plottable_sample(sample)]
        # remove gross outliers and non-valid samples

        last_ambient_date = ambient_samples[-1].date
        # get date after filtering, ie don't plot if there's no new data getting plotted

        date_limits, major_ticks, minor_ticks = create_daily_ticks(ch4_config.days_to_plot)

        if last_ambient_date > ch4_config.last_data_date:

            ch4_config.last_data_date = last_ambient_date
            core_session.merge(ch4_config)

            ambient_dates = [amb.date for amb in ambient_samples]
            ambient_mrs = [amb.peak.mr for amb in ambient_samples]

            with TempDir(rundir / 'plots'):
                name = summit_methane_plot(None, {'Methane': [ambient_dates, ambient_mrs]},
                                    limits={'bottom': 1850, 'top': 2050,
                                            'right': date_limits.get('right', None),
                                            'left': date_limits.get('left', None)},
                                    major_ticks=major_ticks,
                                    minor_ticks=minor_ticks)

                methane_plot = Plot(rundir/'plots'/name, True)  # stage plots to be uploaded
                add_or_ignore_plot(methane_plot, core_session)

            logger.info('New data plots created.')
        else:
            logger.info('No new data found to be plotted.')

        session.close()
        engine.dispose()

        core_session.commit()
        core_session.close()
        core_engine.dispose()
        return True

    except Exception as e:
        logger.error(f'Exception {e.args} occurred in plot_new_data()')
        send_processor_email(PROC, exception=e)
        core_session.close()
        core_engine.dispose()
        session.close()
        engine.dispose()
        return False


async def main():
    """
    Configure a logger and run processes in order, only proceeding if new data is created that warrants running the next
    processes.

    :return: Boolean, True if successful
    """

    try:
        from summit_core import methane_dir as rundir
        from summit_core import configure_logger
        logger = configure_logger(rundir, __name__)
    except Exception as e:
        print(f'Error {e.args} prevented logger configuration.')
        send_processor_email(PROC, exception=e)
        return

    try:
        new_pas = await asyncio.create_task(check_load_pa_log(logger))
        new_logs = await asyncio.create_task(check_load_run_logs(logger))

        if new_pas or new_logs:
            if await asyncio.create_task(match_runs_to_lines(logger)):
                if await asyncio.create_task(match_peaks_to_samples(logger)):
                    await asyncio.create_task(add_one_standard(logger))
                    if await asyncio.create_task(quantify_samples(logger)):
                        await asyncio.create_task(plot_new_data(logger))

        return True

    except Exception as e:
        logger.critical(f'Exception {e.args} caused a complete failure of the CH4 processing.')
        send_processor_email(PROC, exception=e)
        return False


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
