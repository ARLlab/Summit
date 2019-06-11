from datetime import datetime
from summit_core import connect_to_db, voc_dir
from summit_voc import LogFile

engine, session = connect_to_db('sqlite:///summit_voc.sqlite', voc_dir)

logfiles = session.query(LogFile).filter(LogFile.date > datetime(2019, 6, 8)).all()

# The default is WT = 0, Ads = 1
# A=0; B=1

paramBounds = ({'samplepressure1': (1.5, 2),
                'samplepressure2': (7, 10),
                'GCHeadP': (5, 7),
                'GCHeadP1': (9, 12),
                'chamber_temp_start': (20, 28),
                'WT_primary_temp_start': (-35, -25),
                'WT_secondary_temp_start': (20, 35),
                'ads_secondary_temp_start': (20, 35),
                'ads_primary_temp_start': (-35, -25),
                'chamber_temp_end': (20, 28),
                'WT_primary_temp_end': (-35, -25),
                'WT_secondary_temp_end': (20, 35),
                'ads_secondary_temp_end': (20, 35),
                'ads_primary_temp_end': (-35, -25),
                'traptempFH': (-30, 0),
                'GCstarttemp': (35, 45),
                'traptempinject_end': (290, 310),
                'traptempbakeout_end': (310, 330),
                'WT_primary_hottemp': (75, 85),
                'WT_secondary_hottemp': (20, 35),
                'GCoventemp': (190, 210)})

convert_trapnum_to_letter = {False: 'A', True: 'B'}

for log in logfiles:
    print(log.filename)
    WTinuse = bool(log.WTinuse)
    adsTinuse = bool(log.adsTinuse)

    wt_primary = convert_trapnum_to_letter[WTinuse]
    ads_primary = convert_trapnum_to_letter[adsTinuse]

    wt_secondary = convert_trapnum_to_letter[not WTinuse]
    ads_secondary = convert_trapnum_to_letter[not adsTinuse]

    for name, limits in paramBounds.items():

        if '_primary' in name or '_secondary' in name:
            # I know what the primary and secondary traps are by letter
            # I now need to ask the log if NAME (ads_primary_start_temp) is in the limits, BUT
                # that name isn't the log value, which would be adsA_start_temp if A was the primary ads trap
                # SO, replacing _primary with whatever the primary letter is will let us me ask the log for that
            if 'WT_' in name:
                log_attr_name = name.replace('_primary', wt_primary).replace('_secondary', wt_secondary)
            elif 'ads_' in name:
                log_attr_name = name.replace('_primary', ads_primary).replace('_secondary', ads_secondary)
            else:
                print("THIS CAN'T BE HAPPENING.")
                log_attr_name = ''

            log_value = getattr(log, log_attr_name)

            print(limits[0], log_value, limits[1])

            if not (limits[0] <= log_value <= limits[1]):
                # log value not acceptable!
                print(f'Log {log.filename} failed due to parameter {log_attr_name}/{name}')