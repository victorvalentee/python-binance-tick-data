import os
import time
import numpy as np
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from .connection import connect


def _datestr_to_timestamp(string_, format_='%Y-%m-%d', ms=True):
    datetime_ = datetime.strptime(string_, format_)
    timestamp_ = int(datetime.timestamp(datetime_))

    if ms:
        return 1000 * timestamp_
    else:
        return timestamp_


def _store_tick_data(dataset, h5_file, h5_key):
    dataset.to_hdf(
        path_or_buf=h5_file,
        key=h5_key,
        append=True,
        format='table',
        complib='blosc:zstd',
        complevel=9
    )


def _data_prep(trades):
    data_header = ['time', 'price', 'qty']
    prepped_data = pd.DataFrame(trades)[data_header].astype({
        'time': np.uint64,
        'price': np.float32,
        'qty': np.float32,
    })

    return prepped_data


async def create_data_loader(symbol: str, from_date: datetime, to_date: datetime):
    data_loader = TickDataLoader(symbol, from_date, to_date)
    await data_loader._async_init()
    return data_loader


class TickDataLoader:
    def __init__(self, symbol, from_date, to_date):
        self.symbol = symbol
        self.from_date = from_date
        self.to_date = to_date

    async def _async_init(self):
        self.binance_client = await connect()
        time.sleep(0.01)

    async def _get_trade_id_by_timestamp(self, timestamp):
        trades = []
        end_time_offset_ms = 1000

        while not trades:
            trades = await self.binance_client.fetch_aggregate_trades_list(
                symbol=self.symbol,
                start_time=timestamp,
                end_time=timestamp + end_time_offset_ms,
                limit=1
            )

            # Increase time window.
            end_time_offset_ms = end_time_offset_ms + 1000

        first_trade_id = trades[0]['f']
        return first_trade_id

    async def tick_data_to_h5(
            self,
            h5_file_path: str,
            h5_file_key: str,
            del_prior: bool = True,
            request_limit: int = 500
    ):
        if del_prior and os.path.exists(h5_file_path):
            os.remove(h5_file_path)

        start_id = await self._get_trade_id_by_timestamp(int(self.from_date.timestamp() * 1000))
        end_id = await self._get_trade_id_by_timestamp(int(self.to_date.timestamp() * 1000))

        for id in tqdm(range(start_id, end_id, request_limit)):
            trades = await self.binance_client.fetch_old_trades_list(
                symbol=self.symbol,
                from_id=id,
                limit=request_limit
            )

            _store_tick_data(_data_prep(trades), h5_file=h5_file_path, h5_key=h5_file_key)

            # Just to not stress the API too much.
            time.sleep(0.25)

            if trades[-1]['id'] >= end_id:
                return 0
