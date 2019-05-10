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
from summit_daily import check_load_dailies as daily_processor
from summit_daily import plot_dailies
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
		"""
		Processors that are passed a logger will log to /core, others will log to their individual directories/files.
		"""

		dailies = await asyncio.create_task(daily_processor(logger))
		if dailies:
			daily_plots = await asyncio.create_task(plot_dailies(logger))

		vocs = await asyncio.create_task(voc_processor())
		methane = await asyncio.create_task(methane_processor())
		picarro = await asyncio.create_task(picarro_processor())

		if vocs or methane or picarro:
			await asyncio.create_task(check_send_plots(logger))

		errors = await asyncio.create_task(check_for_new_data(logger, active_errors=errors))

		if errors:
			errors = await asyncio.create_task(check_existing_errors(logger, active_errors=errors))

		print('Sleeping...')
		for i in range(20):
			await asyncio.sleep(30)


if __name__ == '__main__':
	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())
