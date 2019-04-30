import asyncio

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

    data = pd.read_excel('Ambient_2019.xlsx', header=None, usecols="A, E:LN")

    data = data.set_index([0])
    data.index = data.index.str.lower()

    engine, session = connect_to_db('sqlite:///summit_voc.sqlite', rundir)

    LogFiles = session.query(LogFile).order_by(LogFile.samplecode)
    NmhcLines = session.query(NmhcLine).order_by(NmhcLine.id)
    GcRuns = session.query(GcRun).order_by(GcRun.id)

    NmhcCorrections = []

    def df_to_corrections(col):
        """
        Loop over columns of ambient_results_master.xlsx.
        This finds the log for each column, then retrieves the NmhcLine that matches it (if any),
        and applies all flags necessary while creating the NmhcCorrection objects.
        """

        code = col['sample code']

        log = LogFiles.filter(LogFile.samplecode == code).first()

        if not log:
            print(f'A log with samplecode {code} was not found in the record, this correction was not processed.')
            return

        run = GcRuns.filter(GcRun.logfile_id == log.id).first()

        if not run:
            print(f'A run matching the log with samplecode {code} was not found, this correction was not processed.')
            return

        line = NmhcLines.filter(NmhcLine.id == run.nmhcline_id).first()

        correction_peaklist = []

        slice_st = 43  # excel is not 0-indexed
        slice_end = 59  # excel is not 0-indexed

        for name, pa, rt in zip(col.index[slice_st:slice_end].tolist(),
                                col[slice_st:slice_end].tolist(),
                                col[slice_st + 54:slice_end + 54].tolist()):
            correction_peaklist.append(Peak(name, pa, rt))

        if not correction_peaklist:
            return

        return NmhcCorrection(line, correction_peaklist, None)


    with session.no_autoflush:
        for col_name in data.columns.tolist():
            col = data.loc[:, col_name]
            NmhcCorrections.append(df_to_corrections(col))

    for correction in NmhcCorrections:
        if correction:
            session.add(correction)

    session.commit()

    NmhcCorrections = session.query(NmhcCorrection).all()  # re-get all added corrections

    for correction in NmhcCorrections:
        if correction:
            print(correction)
            line = session.query(NmhcLine).filter(NmhcLine.correction_id == correction.correction_id).one()
        else:
            continue

        for peak_corr in correction.peaklist:
            # print(peak_corr)
            peak = search_for_attr_value(line.peaklist, 'name', peak_corr.name)

            if not peak:
                peak = search_for_attr_value(line.peaklist, 'rt', peak_corr.rt)

            if not peak:
                print((f"""Peak with name {peak_corr.name} or retention time of {peak.corr.rt} from 
                NmhcCorrection {correction.correction_date} not found in NmhcLine for {line.date}"""))
                # TODO : Peaks not id'd in NmhcLine file won't be present, set to None *here*, or add them on load if not present in line?
                # The latter will make sure all queries to compounds line up....
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
        print(f'Successful peak corrections made to {NmhcLine.date}')


if __name__ == '__main__':
    from summit_core import voc_dir as rundir
    from summit_core import configure_logger

    logger = configure_logger(rundir, 'voc_corrections')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(load_excel_corrections(logger))