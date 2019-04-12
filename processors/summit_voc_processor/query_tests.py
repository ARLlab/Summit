from summit_voc import connect_to_summit_db, LogFile, NmhcLine, GcRun, Datum

homedir = r'C:\Users\arl\Desktop\Summit Processors\Summit VOC Processor'

engine, session, Base = connect_to_summit_db('sqlite:///summit_vocs.sqlite', homedir)

logs = session.query(LogFile).all()
lines = session.query(NmhcLine).all()
runs = session.query(GcRun).all()
data = session.query(Datum).all()

from summit_voc import name_summit_peaks

new_lines = [name_summit_peaks(line) for line in lines]

for item in new_lines:
    for peak in item.peaklist:
        session.merge(peak)
    session.merge(item)
    session.commit()

from summit_voc import get_dates_peak_info
from datetime import datetime

pas, dates = get_dates_peak_info(session, 'n-pentane', 'mr', date_start=datetime(2019, 1, 1))

## The Below is Working

import matplotlib.pyplot as plt

f1 = plt.figure()
ax = f1.gca()

ax.plot(dates, pas, '-o')

mrs, dates = get_dates_peak_info(session, 'ethane', 'mr')

# import matplotlib.pyplot as plt
#
# f1 = plt.figure()
# ax = f1.gca()
#
# ax.plot(dates, mrs, '-o')

# Values look good, aside from minor integration changes reflected in the ambients sheet
