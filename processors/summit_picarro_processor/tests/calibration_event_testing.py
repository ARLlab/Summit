from pathlib import Path
import pandas as pd
import numpy as np
from summit_picarro import connect_to_db, Datum, CalEvent

homedir = Path(r'C:\Users\arl\Desktop\summit_master\processors\summit_picarro_processor\tests')
# homedir = Path(r'C:\Users\brend\PycharmProjects\Summit\processors\summit_picarro_processor\tests')

data_basedir = homedir / '..'

engine, session, Base = connect_to_db('sqlite:///summit_picarro.sqlite', data_basedir)

Base.metadata.create_all(engine)

# LOW, MED, HIGH == MPVs 2, 4, 3

mpv_converter = {1: 'ambient', 2: 'low_std', 4: 'mid_std', 3: 'high_std'}

attr_list = ['id', 'date', 'co', 'co2', 'ch4']

querylist = []
querylist.append([getattr(Datum, attr) for attr in attr_list])

cal_data = {}
for MPV in [2, 3, 4]:
	data = pd.DataFrame(session.query(*attr_list).filter(Datum.mpv_position == MPV).all())
	data['date'] = pd.to_datetime(data['date'])
	cal_data[mpv_converter[MPV]] = data.sort_values(by=['date']).reset_index(drop=True)


from summit_picarro import find_cal_indices

low_data = cal_data['low_std']

cal_indices = find_cal_indices(low_data['date'])  # switch to use datetime instead

prev_ind = 0

cal_events = dict()
for ind in cal_indices:
	ids = low_data['id'].iloc[prev_ind:ind].values.tolist()
	event_data = session.query(Datum).filter(Datum.id.in_(ids)).all()
	cal_events[ind] = CalEvent(event_data, 'low')
	prev_ind = ind+1






