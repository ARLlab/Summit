"""
This alternate form of the check_new_logs function in voc_main_loop is more feature complete, as it includes if/else
statements for the changing parameter bounds based on the state of the H20 Trap and the Absorbent Trap. However,
the code runs slower, and currently every single log in the database has the default state for the traps,
thus I've decided to keep this one as an alternate and let you choose if theres a better way to implement this
feature without losing performence.
"""

import datetime as dt
from pathlib import Path
from datetime import datetime
import summit_core
from summit_core import connect_to_db, TempDir, Config
from summit_core import voc_dir, core_dir
from summit_voc import LogFile
import pandas as pd


engine, session = connect_to_db('sqlite:///Jsummit_voc.sqlite', voc_dir)

core_engine, core_session = connect_to_db('sqlite:///Jsummit_core.sqlite', core_dir)
Config.__table__.create(core_engine, checkfirst=True)

logcheck_config = core_session.query(Config).filter(Config.processor == 'Log Checking').one_or_none()

if not logcheck_config:
    logcheck_config = Config(processor='Log Checking',
                             days_to_plot=21)  # use all default values except processor on init
    core_session.add(logcheck_config)
    core_session.commit()

# Query the VOC Database for the most recent logfile data
recentDate = (session                                                   # open session
                      .query(LogFile.date)                                      # gather date
                      .order_by(LogFile.date.desc())                            # order by desc
                      .first()[0])                                              # grab just the first value

failed = []                                                             # failed var for later

# If the most recent date is greater than the last one, we query for all logs greater than it, save the date of the
# last one, and then apply various actions to them
if recentDate > logcheck_config.last_data_date:
    logfiles = (session
                .query(LogFile.date)  # query DB for dates
                .order_by(LogFile.date.desc())  # order by desc
                .filter(LogFile.date > logcheck_config.last_data_date)  # filter only new ones
                .all())  # get all of them

    lastDate = logfiles[-1]  # identify last log date

    # Loop over logfiles to determine the H20 Trap and Ads Trap in use to define parameter bounds
    for index, file in logfiles.iteritems():
        # TODO: I'm not sure if there's an easy way to make these different paramter bounds depending on the traps?
        # TODO: Maybe with classes, but I'm still inexperience -- if there is a way can you show me?
        # The default is WT = 0, Ads = 1
        if file[index].WTinuse == 0 and file[index].adsTinuse == 1:
            paramBounds = ({                                                            # dictionary of parameters
                'samplepressure1': (1.5, 2),
                'samplepressure2': (7, 10),
                'GCHeadP': (5, 7),
                'GCHeadP1': (9, 12),
                'chamber_temp_start': (20, 28),
                'WTA_temp_start': (-35, -25),
                'WTB_temp_start': (20, 30),
                'adsA_temp_start': (20, 30),
                'adsB_temp_start': (-35, -25),
                'chamber_temp_end': (20, 28),
                'WTA_temp_end': (-35, -25),
                'WTB_temp_end': (20, 30),
                'adsA_temp_end': (20, 30),
                'adsB_temp_end': (-35, -25),
                'traptempFH': (-30, 0),
                'GCstarttemp': (35, 45),
                'traptempinject_end': (290, 310),
                'traptempbakeout_end': (310, 330),
                'WTA_hottemp': (75, 85),
                'WTB_hottemp': (20, 30),
                'GCoventemp': (190, 210)
            })

        # A=0; B=1
        change = {'primary': (-35, -25), 'secondary': (20, 30)}
        checking = {}
        number = {0: 'A', 1: 'B'}

        # If Ads = 0, we switch adsA and adsB starting and ending temperatures
        elif file[index].WTinuse == 0 and file[index].adsTinuse == 0:
            a = [paramBounds['adsA_temp_start'], paramBounds['adsA_temp_end']]
            b = [paramBounds['adsB_temp_start'], paramBounds['adsB_temp_end']]

            paramBounds['adsA_temp_start'] = b[0]
            paramBounds['adsB_temp_start'] = a[0]
            paramBounds['adsA_temp_end'] = b[1]
            paramBounds['adsB_temp_end'] = a[1]

        # If WT = 1, we switch WTA and WTB starting and ending temperatures
        elif file[index].WTinuse == 1 and file[index].adsTinuse == 1:
            a = [paramBounds['WTA_temp_start'], paramBounds['WTA_temp_end']]
            b = [paramBounds['WTB_temp_start'], paramBounds['WTB_temp_end']]

            paramBounds['WTA_temp_start'] = b[0]
            paramBounds['WTB_temp_start'] = a[0]
            paramBounds['WTA_temp_end'] = b[1]
            paramBounds['WTB_temp_end'] = a[1]

        # Last possibility is both are swapped from default
        else:
            a = [paramBounds['WTA_temp_start'], paramBounds['WTA_temp_end']]
            b = [paramBounds['WTB_temp_start'], paramBounds['WTB_temp_end']]

            paramBounds['WTA_temp_start'] = b[0]
            paramBounds['WTB_temp_start'] = a[0]
            paramBounds['WTA_temp_end'] = b[1]
            paramBounds['WTB_temp_end'] = a[1]

            a = [paramBounds['adsA_temp_start'], paramBounds['adsA_temp_end']]
            b = [paramBounds['adsB_temp_start'], paramBounds['adsB_temp_end']]

            paramBounds['adsA_temp_start'] = b[0]
            paramBounds['adsB_temp_start'] = a[0]
            paramBounds['adsA_temp_end'] = b[1]
            paramBounds['adsB_temp_end'] = a[1]

        # Loop through log parameters and identify files outside of acceptable limits
        for log in logfiles:
            failed = []  # failed var for later
            for name, limits in paramBounds.items():
                # Find values below the low limit, or above the high limit
                paramVal = getattr(log, name)
                if not limits[0] <= paramVal <= limits[1]:
                    # Identify the ID of those unacceptable values and append to preallocated list
                    failed.append(name)

            # if failed:
            #     send_logparam_email(log.filename, failed)

        # Update the date of logcheck_config so we don't check same values twice
        logcheck_config.last_data_date = lastDate

# Update the date of logcheck_config so we don't check same values twice
logcheck_config.last_data_date = lastDate

# Merge, commit, close, and dispose of SQL Databases
core_session.merge(logcheck_config)
core_session.commit()
core_session.close()
core_engine.dispose()
session.close()
engine.dispose()

print(failed)
