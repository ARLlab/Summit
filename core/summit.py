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
import asyncio


async def main():
	while True:
		await asyncio.create_task(voc_processor())
		await asyncio.create_task(methane_processor())
		await asyncio.create_task(error_processor())
		print('Sleeping...')
		await asyncio.sleep(5*60)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
