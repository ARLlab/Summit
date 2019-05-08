import os
import asyncio


async def main():
    from summit_core import check_load_dailies, configure_logger

    logger = configure_logger(os.getcwd(), __name__)

    await asyncio.create_task(check_load_dailies(logger))


if __name__ == '__main__':
    await asyncio.create_task(main())

