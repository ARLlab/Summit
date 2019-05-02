"""
This will be the main script for all processing.
"""

import sys

from summit_core import processor_dirs

for d in processor_dirs:
	sys.path.append(str(d))

from voc_main_loop import main as voc_processor
from voc_main_loop import load_excel_corrections
from methane_main_loop import main as methane_processor
from picarro_main_loop import main as picarro_processor
from error_main_loop import check_for_new_data, check_existing_errors
from summit_core import check_send_plots
from summit_errors import send_processor_email
import asyncio

ambient_sheetpath = r'Z:\Data\Summit_GC\Summit_GC_2019\NMHC_results\Ambient_2019.xlsx'
# TODO include in JSON filepaths instead


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

		vocs = await asyncio.create_task(voc_processor())
		methane = await asyncio.create_task(methane_processor())
		picarro = await asyncio.create_task(picarro_processor())

		excel_loads = await asyncio.create_task(load_excel_corrections(ambient_sheetpath, logger))
		# TODO : Change to call only once after 12:30AM each day (and config-store a date for plots?)

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
