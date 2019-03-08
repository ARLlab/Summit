import os, logging
from pathlib import Path

import sqlalchemy

def configure_logger():
	logfile = Path(os.getcwd()) / 'processor_logs/summit_picarro.log'
	logger = logging.getLogger('summit_voc')
	logger.setLevel(logging.DEBUG)
	fh = logging.FileHandler(logfile)
	fh.setLevel(logging.DEBUG)

	ch = logging.StreamHandler()
	ch.setLevel(logging.INFO)

	formatter = logging.Formatter('%(asctime)s -%(levelname)s- %(message)s')

	[H.setFormatter(formatter) for H in [ch, fh]]
	[logger.addHandler(H) for H in [ch, fh]]

	return logger


logger = configure_logger()

def list_files_recur(path):
	from pathlib import Path
	files = []
	for file in path.rglob('*'):
		files.append(file.name)

	return files

path = Path(os.getcwd())

lst = list_files_recur(path)

lst[:] = [file for file in lst if '.dat' in file]

