import os
import asyncio
from datetime import datetime
from binance_utils.get_tick_data import create_data_loader


async def main():
    data_loader = await create_data_loader(
        symbol='BTCUSDT',
        from_date=datetime(2019, 1, 1),
        to_date=datetime(2020, 1, 1)
    )

    await data_loader.tick_data_to_h5(
        h5_file_path=f'{os.getcwd()}/data/ticks.h5',
        h5_file_key='BTCUSDT_2019',
        del_prior=True
    )

if __name__ == '__main__':
    asyncio.run(main())

