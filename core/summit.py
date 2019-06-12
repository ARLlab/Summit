"""
This is the main script for all processing.

The main() functions of individual modules are loaded as whole processors, so VOCs are run all at once, the Picarro is
run all at once, etc.

The system path is first extended to account for loading modules from their own directories; so long as the relative
structure is the same and the parent folder is named "Summit" (per cloning from Git), or "summmit_master",
the original directory name, the will find the correct directories and add them to the system path.

Log and data files are sync to several folders in the FTP directory, and move_log_files() moves these to Summit/data/...

move_log_files() runs asynchronously, every five minutes, and is scheduled prior to main(), so it works independently.
It checks for and transfers any new files before sleeping for another five minutes.

All other processors run sequentially (as defined in main()), every 10 minutes.

Sleeps are blocked into 30s periods to permit keyboard interrupts and easy restarts of the whole processing sequence.
"""

import sys

from summit_core import processor_dirs

for d in processor_dirs:
    sys.path.append(str(d))

from voc_main_loop import main as voc_processor
from methane_main_loop import main as methane_processor
from methane_main_loop import dual_plot_methane
from picarro_main_loop import main as picarro_processor
from error_main_loop import check_for_new_data, check_existing_errors
from summit_daily import check_load_dailies as daily_processor
from summit_daily import plot_dailies
from summit_core import check_send_plots, move_log_files
from summit_errors import send_processor_email
import asyncio


async def main(logger):
    errors = []  # initiate with no errors

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

        if methane or picarro:
            await asyncio.create_task(dual_plot_methane(logger))

        if vocs or methane or picarro:
            await asyncio.create_task(check_send_plots(logger))

        errors = await asyncio.create_task(check_for_new_data(logger, active_errors=errors))

        if errors:
            errors = await asyncio.create_task(check_existing_errors(logger, active_errors=errors))

        print('Sleeping...')
        for i in range(40):
            await asyncio.sleep(30)


if __name__ == '__main__':

    try:
        from summit_core import methane_dir as rundir
        from summit_core import configure_logger

        logger = configure_logger(rundir, __name__)

    except Exception as e:
        print(f'Error {e.args} prevented logger configuration.')
        send_processor_email('MAIN', exception=e)
        raise e

    # Commented Off so I don't mess up the website at all in any way
    loop = asyncio.get_event_loop()
    # loop.create_task(move_log_files(logger))
    # loop.create_task(main(logger))

    loop.run_forever()
