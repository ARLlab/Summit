import os
import asyncio
from pathlib import Path

from summit_daily import check_load_dailies


if __name__ == '__main__':
    from summit_core import configure_logger, core_dir

    logger = configure_logger(core_dir, __name__)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(check_load_dailies(logger))

