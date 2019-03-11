import os, random
from pathlib import Path
import pandas as pd
from summit_picarro import get_all_data_files, connect_to_db, Datum, DataFile

rundir = Path(r'C:\Users\arl\Desktop\summit_master\processors\summit_picarro_processor\tests')

data_basedir = rundir / '..'

engine, session, Base = connect_to_db('sqlite:///summit_picarro.sqlite', data_basedir)

Base.metadata.create_all(engine)

files = get_all_data_files(data_basedir)

flist = []
while len(flist) < int(len(files)/10):
	flist.append(random.randint(0, len(files)-1))

print(f'{len(flist)} data files chosen at random.')

file_list = []
data_list = []
for ind in flist:
	df = pd.read_csv(files[ind], delim_whitespace=True)
	df_list = df.to_dict('records')

	for line in df_list:
		data_list.append(Datum(line))

	file_list.append(DataFile(files[ind]))

print(f'{len(data_list)} Datum objects created.')

for d in data_list:
	session.add(d)
session.commit()

print(f'{len(data_list)} Datum objects committed.')

for file in file_list:
	session.add(file)
session.commit()

print(f'{len(file_list)} DataFile objects committed.')













