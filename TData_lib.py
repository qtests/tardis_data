import sys; 
sys.path.extend(['../crypto_trading'])

import os
import asyncio

from pathlib import Path

from market.tardis import Tardis, TardisType, BitMEX, instrument_info, download_book_change_tasks, download_task

from calendar import monthrange, isleap
import datetime as dt


# https://stackoverflow.com/questions/7045920/how-to-calculate-the-number-of-days-in-the-years-between-2-dates-in-python
def days_in_year(year):
    return 365 + isleap(year)


def days_in_months(year, nof_months=7):
    total_days = 0
    for m in range(1, nof_months+1): 
        _, temp = monthrange(year, m)
        total_days += temp
    return (total_days)


def months_in_year(year):
    now = dt.datetime.now()
    if year == now.year:
        return (now.month - 1)
    elif year < now.year:
        return (12)
    else:
        return (None)


async def download_year (symbol, dyear = 2021, dmonths = 7, exchange = "binance-futures", dataType = "trade_bar_1m"):

    if dyear is None or dmonths is None:
        return
    
    start_date = f'{dyear}-01-01T00:00:00.000Z'

    if dmonths == 12:
        num_days = days_in_year(dyear)
    else:
        num_days = days_in_months(dyear, dmonths)

    task = download_task(exchange, symbol, dataType, start_date, num_days)
    # task = [download_task(exchange, s, dataType, start_date, num_days) for s in symbols]
    await asyncio.gather(task)


def make_new_name(old_name):
    _, _, ticker, year, _ = old_name.split("_")
    return (("_").join([ticker, year]))


def rename_files(folder, f_new_name):
    for count, f in enumerate(os.listdir(folder)):
        f_name, f_ext = os.path.splitext(f)

        f_name = f_new_name(f_name)
    
        new_name = f'{f_name}{f_ext}'
        os.rename(Path(folder).joinpath(f), Path(folder).joinpath(new_name))

