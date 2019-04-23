"""
This will be the main script for all processing.

TODO: Some random counter variables and such are not stored in DB, those should go to config tables per processor

_______Things that need config/DB Storage:
Best way to do this? ORM seems overpowered and possibly not helpful?

VOCs:
pa_file_size: bytesize of the pa file to determine if it's newer
start_line: line to start reading from; saves rejecting thousands of already-in-db lines later
data_len: Length of data for VOC plotting to determine new data; should be changed to most recent data date instead
days_to_plot:

CH4:
start_size, start_line: bytesize of the PA file and line to start reading from
data_len: Length of data for VOC plotting to determine new data; should be changed to most recent data date instead
days_to_plot:

Picarro:
days_to_plot:
date_ago: this even used?
last_data_point: should be date





"""
import sys

from summit_core import processor_dirs

for d in processor_dirs:
	sys.path.append(str(d))

from voc_main_loop import main as voc_processor
from methane_main_loop import main as methane_processor
from error_main_loop import check_for_new_data, check_existing_errors
from summit_core import check_send_plots
from summit_errors import send_processor_email
import asyncio


async def main():
	try:
		from summit_core import methane_dir as rundir
		from summit_core import configure_logger
		logger = configure_logger(rundir, __name__)
		errors = []  # initiate with no errors
	except Exception as e:
		print(f'Error {e.args} prevented logger configuration.')
		send_processor_email('MAIN', exception=e)
		return

	while True:

		vocs = await asyncio.create_task(voc_processor())
		methane = await asyncio.create_task(methane_processor())


		if vocs or methane:
			await asyncio.create_task(check_send_plots(logger))

		errors = await asyncio.create_task(check_for_new_data(logger, active_errors=errors))

		if errors:
			errors = await asyncio.create_task(check_existing_errors(errors))

		print('Sleeping...')
		await asyncio.sleep(15*60)


if __name__ == '__main__':
	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())
