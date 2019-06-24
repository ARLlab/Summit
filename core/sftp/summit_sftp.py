import os
import json
from pathlib import Path

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

CORE_DIR = Path('/home/user/Projects/syncfolder')
REMOTE_BASE_PATH = '/home/other_user/Projects/syncfolder'

Base = declarative_base()


class LocalFile(Base):
	__tablename__ = 'localfiles'

	remote_id = Column(Integer, ForeignKey('remotefiles.id'))
	remote = relationship('RemoteFile', uselist=False, back_populates='local')

	id = Column(Integer, primary_key=True)
	st_mtime = Column(Integer)
	path = Column(String)
	relpath = Column(String)

	def __init__(self, st_mtime, path):
		self.st_mtime = st_mtime
		self.path = path
		self.relpath = path.replace(str(CORE_DIR), '')


class RemoteFile(Base):
	__tablename__ = 'remotefiles'

	local = relationship('LocalFile', uselist=False, back_populates='remote')

	id = Column(Integer, primary_key=True)
	st_mtime = Column(Integer)
	path = Column(String)
	relpath = Column(String)

	def __init__(self, st_mtime, path):
		self.st_mtime = st_mtime
		self.path = path
		self.relpath = path.replace(REMOTE_BASE_PATH, '')


def search_for_attr_value(obj_list, attr, value):
	"""
	Finds the first (not necesarilly the only) object in a list, where its
	attribute 'attr' is equal to 'value', returns None if none is found.
	:param obj_list: list, of objects to search
	:param attr: string, attribute to search for
	:param value: mixed types, value that should be searched for
	:return: obj, from obj_list, where attribute attr matches value
		**** warning: returns the *first* obj, not necessarily the only
	"""
	return next((obj for obj in obj_list if getattr(obj, attr, None) == value), None)


def connect_to_sftp():
	"""
	Uses paramiko to create a connection to Brendan's instance. Relies on authetication information from a JSON file.
	:return: SFTP_Client
	"""
	import paramiko
	key = paramiko.RSAKey.from_private_key_file(r'/home/brendan/keys/lightsail.pem')
	client = paramiko.SSHClient()
	client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	with open('server_info.json', 'r') as file:
		server_info = json.loads(file.read())
	server_info['pkey'] = key
	client.connect(**server_info)
	return client.open_sftp()


def list_remote_files(con, directory):
	"""
	List the files and folders in a remote directory using an active SFTPClient from Paramiko
	:param con: SFTPClient, an active connection to an SFTP server
	:param directory: string, the directory to search
	:return: (generator, generator), the files and directories as separate generators
	"""
	from stat import S_ISDIR

	print(directory)
	all_files = [file for file in con.listdir_attr(directory)]

	files = []
	dirs = []

	for file in all_files:
		if S_ISDIR(file.st_mode):
			file.path = directory + f'/{file.filename}'  # ad-hoc add the remote filepath since Paramiko ignores this?!
			dirs.append(file)
		else:
			file.path = directory + f'/{file.filename}'  # ad-hoc add the remote filepath since Paramiko ignores this?!
			files.append(file)

	files = (file for file in files)
	dirs = (dir for dir in dirs)

	return (files, dirs)


def list_remote_files_recur(con, directory, files=None, dirs=None):
	"""
	List all remote files, recursively through all directories in the given directory.

	:param con: SFTPClient object, an active SFTP connection
	:param directory: directory to search for files and folders
	:param files: optional, generator; file generator to append to with chain()
	:param dirs: optional, generator; directory generator to append to with chain()
	:return: files; a generator of all the SFTPAttribute files
	"""
	from itertools import chain

	if not files and not dirs:
		files, dirs = list_remote_files(con, directory)

	dirs, dirs_is_empty = gen_isempty(dirs)

	if not dirs_is_empty:
		directory_to_check = None
		for d in dirs:
			directory_to_check = d.path
			new_files, new_dirs = list_remote_files(con, directory_to_check)
			files = chain(files, new_files)
			dirs = chain(dirs, new_dirs)
		return list_remote_files_recur(con, directory_to_check, files=files, dirs=dirs)
	else:
		return files


def scan_and_create_dir_tree(path, file=True):
	"""
	Creates all the necessary directories for the file at the end of path to be created.
	:param path: Path, must end with a filename, else the final directory won't be created
	:param file: Boolean, does the given path end with a file? If not, path.parts[-1] will be created
	:return: None
	"""

	parts = path.parts
	path_to_check = Path(parts[0])

	for i in range(1, len(parts)):
		if not path_to_check.exists():
			path_to_check.mkdir()
		path_to_check = path_to_check / parts[i]

	if file:
		pass
	else:
		if not path_to_check.exists():
			path_to_check.mkdir()


def gen_isempty(gen):
	"""
	Returns an identical generator and False if it has at least one value, True if it's empty
	:param gen: generator, any generator
	:return: (generator, Boolean), returns the same generator, and True if empty, False if not
	"""
	try:
		item = next(gen)

		def my_generator():
			yield item
			yield from gen

		return my_generator(), False
	except StopIteration:
		return (_ for _ in []), True


def retrieve_new_files(logger):
	from summit_core import connect_to_db, list_files_recur, split_into_sets_of_n

	logger.info('Running retrieve_new_files()')

	con = connect_to_sftp()
	engine, session = connect_to_db('sqlite:///zugspitze.sqlite', CORE_DIR)

	for path in ['folder1', 'folder2', 'folder3']:
		logger.info(f'Processing {path} files.')

		local_path = CORE_DIR / path
		remote_path = REMOTE_BASE_PATH + f'/{path}'

		all_remote_files = list_remote_files_recur(con, remote_path)  # get a list of all SFTPAttributes + paths

		all_local_files = [str(p) for p in list_files_recur(local_path)]  # get all local file paths

		new_remote_files = []
		for remote_file in all_remote_files:
			new_remote_files.append(RemoteFile(remote_file.st_mtime, remote_file.path))
		# create DB objects for all remote paths

		new_local_files = []
		for remote_file in all_local_files:
			new_local_files.append(LocalFile(os.stat(remote_file).st_mtime, remote_file))
		# create DB objects for all local paths

		remote_sets = split_into_sets_of_n([r.path for r in new_remote_files], 750)  # don't exceed 1K sqlite var limit
		local_sets = split_into_sets_of_n([l.path for l in new_local_files], 750)

		# loop through remote, then local filesets to check against DB and commit any new ones
		for Filetype, filesets, new_files in zip([RemoteFile, LocalFile],
												 [remote_sets, local_sets],
												 [new_remote_files, new_local_files]):
			paths_in_db = []
			for set_ in filesets:
				in_db = session.query(Filetype.path).filter(Filetype.path.in_(set_)).all()
				if in_db:
					paths_in_db.extend(in_db)

			for file in new_files:
				if file.path in paths_in_db:
					file_in_db = session.query(Filetype).filter(Filetype.path == file.path).one_or_none()
					if file.st_mtime > file_in_db.st_mtime:
						file_in_db.st_mtime = file.st_mtime
						session.merge(file_in_db)
				else:
					session.add(file)
			session.commit()  # commit at the end of each filetype

		# local and remote files are now completely up-to-date in the database
		files_to_retrieve = []
		remote_files = session.query(RemoteFile).order_by(RemoteFile.relpath).all()
		local_files = session.query(LocalFile).order_by(LocalFile.relpath).all()

		for remote_file in remote_files:
			if remote_file.local is None:
				local_match = search_for_attr_value(local_files, 'relpath', remote_file.relpath)
				if local_match:
					remote_file.local = local_match
					if remote_file.st_mtime > local_match.st_mtime:
						files_to_retrieve.append(remote_file)  # add the remote file to download if st_mtime is greater
				else:
					files_to_retrieve.append(remote_file)  # add the remote file if there's no local copy (create later)
			else:
				if remote_file.st_mtime > remote_file.local.st_mtime:
					files_to_retrieve.append(remote_file)

		logger.info(f'Remote files: {len(remote_files)}')
		logger.info(f'Local files: {len(local_files)}')
		logger.info(f'{len(files_to_retrieve)} file need updating or retrieval.')

		ct = 0
		for remote_file in files_to_retrieve:
			if remote_file.local is not None:
				con.get(remote_file.path, remote_file.local.path)  # get remote file and put in the local's path

				remote_file.local.st_mtime = remote_file.st_mtime  # update, then merge
				session.merge(remote_file)

				logger.info(f'Remote file {remote_file.relpath} was updated.')
				ct += 1
			else:
				new_local_path = CORE_DIR / remote_file.relpath.lstrip('/')

				scan_and_create_dir_tree(new_local_path)  # scan the path and create any needed folders

				new_local_path = str(new_local_path)  # revert to string
				con.get(remote_file.path, new_local_path)  # get file and put in it's relative place

				new_local = LocalFile(remote_file.st_mtime, new_local_path)
				new_local.remote = remote_file

				session.add(new_local)  # create, relate, and add the local file that was transferred
				session.merge(remote_file)

				logger.info(f'Remote file {remote_file.relpath} was retrieved and added to local database.')
				ct += 1

			if ct % 100 == 0:
				session.commit()  # routinely commit files in batches of 100

		session.commit()

		session.close()
		engine.dispose()
