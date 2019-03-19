from pathlib import Path
import pandas as pd
import numpy as np
from summit_picarro import connect_to_db, Datum, CalEvent, MasterCal

homedir = Path(r'C:\Users\arl\Desktop\summit_master\processors\summit_picarro_processor\tests')
# homedir = Path(r'C:\Users\brend\PycharmProjects\Summit\processors\summit_picarro_processor\tests')

data_basedir = homedir / '..'

engine, session, Base = connect_to_db('sqlite:///summit_picarro.sqlite', data_basedir)

Base.metadata.create_all(engine)

mastercals = session.query(MasterCal).all()

for mc in mastercals:
	mc.create_curve()

