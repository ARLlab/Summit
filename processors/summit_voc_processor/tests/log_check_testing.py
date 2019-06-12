from datetime import datetime
from summit_core import connect_to_db, voc_dir
from summit_voc import LogFile

engine, session = connect_to_db('sqlite:///summit_voc.sqlite', voc_dir)

logfiles = session.query(LogFile).filter(LogFile.date > datetime(2019, 3, 8)).all()

paramBounds = ({'samplepressure1': (1.5, 2.65),
                'samplepressure2': (6.5, 10),
                'GCHeadP': (5, 7.75),
                'GCHeadP1': (9, 13),
                'chamber_temp_start': (18, 30),
                'WT_primary_temp_start': (-35, -24),
                'WT_secondary_temp_start': (18, 35),
                'ads_secondary_temp_start': (18, 35),
                'ads_primary_temp_start': (-35, -24),
                'chamber_temp_end': (18, 30),
                'WT_primary_temp_end': (-35, -24),
                'WT_secondary_temp_end': (15, 35),
                'ads_secondary_temp_end': (15, 35),
                'ads_primary_temp_end': (-35, -24),
                'traptempFH': (-35, 0),
                'GCstarttemp': (35, 45),
                'traptempinject_end': (285, 310),
                'traptempbakeout_end': (310, 335),
                'WT_primary_hottemp': (75, 85),
                'WT_secondary_hottemp': (20, 35),
                'GCoventemp': (190, 210)})

"""
A Note About Log Parameter Checks:

The "real" names of water trap and ads trap attributes are WTA_temp_start, adsB_temp_end etc, BUT the acceptable values 
for each depend on which trap is the active one. The active traps should cool to sub-zero temperatures, and the
inactive traps should remain around ambient.

Because of this, the boundary parameters are listed as WT_primary_temp_start, ads_secondary_temp_end etc.
The attribute that should be retrieved from each logfile is then constructed by replacing _primary or _secondary in the 
boundary name with A or B depending on the active water and adsorbent traps. This allows:
getattr(logfile, log_name) to get the primary or secondary trap information and check that it's within limits. 
"""

for log in logfiles:
    wt_primary = 'A' if log.WTinuse is 0 else 'B'
    ads_primary = 'A' if log.adsTinuse is 0 else 'B'

    wt_secondary = 'A' if wt_primary is 'B' else 'B'
    ads_secondary = 'A' if ads_primary is 'B' else 'B'

    for name, limits in paramBounds.items():

        if '_primary' in name or '_secondary' in name:
            if 'WT_' in name:
                log_name = name.replace('_primary', wt_primary).replace('_secondary', wt_secondary)
            elif 'ads_' in name:
                log_name = name.replace('_primary', ads_primary).replace('_secondary', ads_secondary)
            else:
                print("THIS CAN'T BE HAPPENING.")
                log_name = None
        else:
            log_name = name  # pass the name on if it's not a water/ads trap special case

        log_value = getattr(log, log_name)

        if not (limits[0] <= log_value <= limits[1]):
            # log value not acceptable!
            print(limits[0], log_value, limits[1])
            if log_name != name:  # give second name too if not the same
                print(f'Log {log.filename} failed due to parameter {name}/{log_name}')
            else:
                print(f'Log {log.filename} failed due to parameter {name}')