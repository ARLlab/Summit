"""
This will be the main script for all processing.

TODO: Flow chart for processes


TODO: Make data file paths importable from a json file
TODO: Fix all errors from logs
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
		await asyncio.sleep(15*60)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
