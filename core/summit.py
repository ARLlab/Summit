"""
This will be the main script for all processing.
"""

import sys

from summit_core import processor_dirs

for d in processor_dirs:
	sys.path.append(str(d))

from voc_main_loop import main as voc_processor
from methane_main_loop import main as methane_processor
from picarro_main_loop import main as picarro_processor
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
		picarro = await asyncio.create_task(picarro_processor())

		if vocs or methane or picarro:
			await asyncio.create_task(check_send_plots(logger))

		errors = await asyncio.create_task(check_for_new_data(logger, active_errors=errors))

		if errors:
			errors = await asyncio.create_task(check_existing_errors(logger, active_errors=errors))

		print('Sleeping...')
		await asyncio.sleep(9*60)


if __name__ == '__main__':
	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())
