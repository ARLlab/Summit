import pandas as pd
import datetime as dt

from pathlib import Path
from datetime import datetime

from summit_methane import GcRun, add_formulas_and_format_sheet
from summit_core import connect_to_db, append_df_to_excel
from summit_core import methane_dir, data_file_paths

methane_sheet = data_file_paths.get('methane_sheet', None)

if not methane_sheet:
    pass
    # TODO: ERROR!

engine, session = connect_to_db('sqlite:///summit_methane.sqlite', methane_dir)

runs_for_this_year = session.query(GcRun).filter(GcRun.date.between(datetime(2019, 1, 1), datetime.now())).all()

col_list = ['date', 'filename', 'peak1', 'peak2', 'mr1', 'mr2', 'run_median', 'run_rsd', 'std_median', 'std_rsd']

master_df = pd.DataFrame(index=None, columns=col_list)

for run in runs_for_this_year:
    df = pd.DataFrame(index=range(1,6),columns=col_list)
    df['date'][1] = run.date
    df['filename'][1] = run.logfile.name

    # The below can be turned on to copy peak information from the automatic integrations into the spreadsheet

    # peaks1 = [sample.peak for sample in run.samples if sample.sample_num in [0,2,4,6,8]]
    # peaks2 = [sample.peak for sample in run.samples if sample.sample_num in [1,3,5,7,9]]
    #
    # df.loc[0:5, 'peak1'] = [(peak.pa if peak else None) for peak in peaks1]
    # df.loc[0:5, 'peak2'] = [(peak.pa if peak else None) for peak in peaks2]

    master_df = master_df.append(df)

append_df_to_excel(methane_sheet, master_df, **{'index': False})

add_formulas_and_format_sheet(methane_sheet)
