from summit_errors import Error, EmailTemplate, ProccessorEmail, NewDataEmail, sender
from datetime import datetime
import datetime as dt
from pathlib import Path
import os, json
import asyncio

from summit_core import configure_logger
from summit_core import error_dir as rundir

logger = configure_logger(rundir, __name__)

active_errors = []  # Errors do not persist when loops are not running


def new_data_found(processor, last_data_time):
	if get_last_processor_date(processor) > last_data_time:
		return True
	else:
		return False


def get_last_processor_date(processor):
	"""
	Retrieves the latest high-level date for the specified processor. It looks at GcRuns for VOCs (complete runs),
	5-second Datums for the Picarro, and matched GcRuns for methane.
	:param processor: str, in ['voc', 'picarro', 'methane']
	:param directory:
	:return: datetime, date of last data point for the specified processor
	"""

	from summit_core import connect_to_db, TempDir
	from sqlalchemy.orm.exc import MultipleResultsFound

	if processor is 'voc':
		from summit_core import voc_dir as directory
		from summit_voc import GcRun as DataType
	elif processor is 'picarro':
		from summit_core import picarro_dir as directory
		from summit_picarro import Datum as DataType
	elif processor is 'methane':
		from summit_core import methane_dir as directory
		from summit_methane import GcRun as DataType
	else:
		logger.error('Invalid processor supplied to get_last_processor_date()')
		assert False, 'Invalid processor supplied to get_last_processor_date()'

	with TempDir(directory):
		engine, session = connect_to_db(f'sqlite:///summit_{processor}.sqlite', directory)
		val = session.query(DataType.date).order_by(DataType.date.desc()).first()[0]

	session.close()
	engine.dispose()

	return val


def matching_error(error_list, reason, processor):
	"""
	There's a matching error if any error in the list has the same processor and reason associated with it.

	:param error_list: list, of Error objects
	:param reason: str, reason for the error
	:param processor: str, in ['voc', 'methane', 'picarro']
	:return: boolean, True if there's a matching error
	"""
	return next((True for err in error_list if
				(err.email_template.processor == processor and err.reason == reason)), False)


async def check_for_new_data(directory, sleeptime):

	reason = 'no new data'

	while True:
		# TODO : The time limits generated below are gross estimates
		for proc, time_limit in zip(['voc', 'picarro', 'methane'], [dt.timedelta(hours=hr) for hr in [8,3,5]]):

			last_data_time = get_last_processor_date(proc)

			if datetime.now() - last_data_time > time_limit:

				if matching_error(active_errors, reason, proc):
					continue
				else:
					active_errors.append(Error(reason, new_data_found, NewDataEmail(sender, proc, last_data_time)))

		await asyncio.sleep(sleeptime)


async def check_existing_errors(directory, sleeptime):
	pass


def main():
	pass


if __name__ == '__main__':
	main()







def main():
	loop = asyncio.get_event_loop()
	loop.create_task(check_for_new_voc_data(rundir, 10))

	loop.run_forever()


if __name__ == '__main__':
	main()