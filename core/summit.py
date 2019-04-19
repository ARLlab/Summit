"""
This will be the main script for all processing.

TODO: Flow chart for processes

TODO: Make plot files a JSON list that's updated with filenames? Could be DB table...so no un-updated files are sent...
TODO: Some random counter variables and such are not stored in DB, those should go to config tables per processor
TODO: FTP Transfers to Taylor Drive
"""
import sys

from summit_core import processor_dirs

for d in processor_dirs:
	sys.path.append(str(d))

from voc_main_loop import main as voc_processor
from methane_main_loop import main as methane_processor
from error_main_loop import main as error_processor
from summit_core import check_send_plots
from summit_errors import send_processor_email
import asyncio


async def main():
	try:
		from summit_core import methane_dir as rundir
		from summit_core import configure_logger
		logger = configure_logger(rundir, __name__)
	except Exception as e:
		print(f'Error {e.args} prevented logger configuration.')
		send_processor_email('MAIN', exception=e)
		return

	while True:
		vocs = await asyncio.create_task(voc_processor())
		methane = await asyncio.create_task(methane_processor())
		await asyncio.create_task(error_processor())

		if vocs or methane:
			await asyncio.create_task(check_send_plots(logger))

		print('Sleeping...')
		await asyncio.sleep(15*60)


if __name__ == '__main__':
	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())
