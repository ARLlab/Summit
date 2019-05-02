import asyncio


def check_cols(name):
    """
    Callable mini-function passed to pd.read_excel(usecols=function).
    Grab the first column to use as an index, then all columns starting with column 4.
    """
    return True if name == 0 or name >= 4 else False


def correction_from_df_column(col, logfiles, nmhc_lines, gc_runs, logger):
    """
    Loop over columns of ambient_results_master.xlsx.
    This finds the log for each column, then retrieves the NmhcLine that matches it (if any),
    and applies all flags necessary while creating the NmhcCorrection objects.
    """
    from summit_voc import Peak, LogFile, GcRun, NmhcLine, NmhcCorrection

    code = col.iloc[0]

    log = logfiles.filter(LogFile.samplecode == code).one_or_none()

    if not log:
        logger.warning(f'A log with samplecode {code} was not found in the record, this correction was not processed.')
        return

    run = gc_runs.filter(GcRun.logfile_id == log.id).one_or_none()

    if not run:
        logger.warning(f'A run matching the log with samplecode {code} was not found, this correction was not processed.')
        return

    line = nmhc_lines.filter(NmhcLine.id == run.nmhcline_id).one_or_none()

    correction_peaklist = []

    slice_st = 43  # excel is not 0-indexed
    slice_end = 59  # excel is not 0-indexed

    for name, pa, rt in zip(col.index[slice_st:slice_end].tolist(),
                            col[slice_st:slice_end].tolist(),
                            col[slice_st + 54:slice_end + 54].tolist()):
        correction_peaklist.append(Peak(name, pa, rt))

    if not correction_peaklist or not line:
        return None

    return NmhcCorrection(line, correction_peaklist, None)


async def load_excel_corrections(logger):

    try:
        import pandas as pd
        from pathlib import Path
        from summit_voc import Peak, LogFile, NmhcLine, NmhcCorrection, GcRun
        from summit_core import connect_to_db, search_for_attr_value
        from summit_core import voc_dir as rundir
    except ImportError as e:
        logger.error('ImportError occurred in load_excel_corrections()')
        return False

    data = pd.read_excel('Ambient_2019.xlsx', header=None, usecols=check_cols).dropna(axis=1, how='all')

    data = data.set_index([0])  # set first row of df to the index
    data.index = data.index.str.lower()
    data = data[data.columns[:-1]]  # drop last row of DF (the one with 'END' in it)

    engine, session = connect_to_db('sqlite:///summit_voc.sqlite', rundir)

    logfiles = session.query(LogFile).order_by(LogFile.samplecode)
    nmhc_lines = session.query(NmhcLine).filter(NmhcLine.correction_id == None).order_by(NmhcLine.id)
    gc_runs = session.query(GcRun).order_by(GcRun.id)

    nmhc_corrections = []

    corrections_in_db = session.query(NmhcCorrection).all()
    correction_dates_in_db = [c.date for c in corrections_in_db]

    with session.no_autoflush:
        for col_name in data.columns.tolist():
            col = data.loc[:, col_name]
            nmhc_corrections.append(correction_from_df_column(col, logfiles, nmhc_lines, gc_runs, logger))

    for correction in nmhc_corrections:
        if correction:
            if correction.date not in correction_dates_in_db:
                session.add(correction)
                logger.info(f'Correction for {correction.date} added.')

    session.commit()

    nmhc_corrections = session.query(NmhcCorrection).filter(NmhcCorrection.status == 'unapplied').all()
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
            peak_by_name = search_for_attr_value(line.peaklist, 'name', peak_corr.name)
            peak_by_rt = search_for_attr_value(line.peaklist, 'rt', peak_corr.rt)

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
                    logger.warning(f"Peak with name {peak_corr.name} or retention time of {peak_corr.rt} from "
                                   + f"NmhcCorrection {correction.date} not found in NmhcLine for {line.date}")
                    continue

            if peak.pa != peak_corr.pa:
                peak.pa = peak_corr.pa
                peak.rt = peak_corr.rt
                peak.rev = peak.rev + 1  # Sqlite *does not* like using += notation

        correction.status = 'applied'

        line.nmhc_corr_con = correction
        correction.correction_id = line

        session.merge(correction)
        session.merge(line)
        logger.info(f'Successful peak corrections made to {line.date}')
        session.commit()


if __name__ == '__main__':
    from summit_core import voc_dir as rundir
    from summit_core import configure_logger

    logger = configure_logger(rundir, 'voc_corrections')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(load_excel_corrections(logger))