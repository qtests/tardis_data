import sys; 
print('Python %s on %s' % (sys.version, sys.platform))

# Python 3.9.10 (tags/v3.9.10:f2f3f53, Jan 17 2022, 15:14:21) [MSC v.1929 64 bit (AMD64)] on win32

sys.path.extend(['../crypto_trading'])

import os
import asyncio
import pandas as pd

import logging
from pathlib import Path

from market.tardis import Tardis, TardisType, BitMEX, instrument_info, download_book_change_tasks, download_task
from market.tardis import to_zorro_t6_compatible_csv

from calendar import monthrange, isleap
import datetime as dt

import subprocess as sp



print(f"\n\n{dt.date.today()}--------- Starting to download ---------")

# Init
#
all_years = [2021, 2022]
all_symbols = [ "BTCUSDT", "ETHUSDT", "ATOMUSDT", "DOTUSDT", "SOLUSDT", "ADAUSDT", 
                    "EOSUSDT", "BNBUSDT", "XMRUSDT", "DOGEUSDT", "GRTUSDT", "MKRUSDT", "AAVEUSDT"]


Tardis.logger().setLevel(logging.INFO)
cache_dir = Tardis().cache_dir

zorro_csv_dir = Path(os.path.join(cache_dir)).parent.joinpath("zorro")
zorro_t6_dir = Path(os.path.join(cache_dir)).parent.joinpath("zorro_t6")
zorro_exe = r'C:\co\zorro\Zorro2444\Zorro.exe'
zorro_stay = False

if not os.path.exists(zorro_csv_dir):
    os.makedirs(zorro_csv_dir)

if not os.path.exists(zorro_t6_dir):
    os.makedirs(zorro_t6_dir)

# binance = instrument_info("binance-futures")
# b_instruments = pd.DataFrame([
#         instr for instr in binance if instr["datasetId"].endswith("USDT")
#     ]).set_index("id").sort_index()
# print (b_instruments)


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


for year in all_years:

    # Data download
    for symb in all_symbols:
        asyncio.run(download_year(symbol=symb, dyear=year, dmonths=months_in_year(year)))
    
    # New content
    new_content = Tardis().stored_content()
    new_df = pd.DataFrame(new_content)
    for idx, arow in new_df[['meta_file', 'symbols']].iterrows():
        r_meta_file, r_symbols = arow[0], arow[1]

        print ( "Converting to Zorro: ", r_symbols[0], " ", r_meta_file )
        
        start_date = f'{year}-01-01T00:00:00.000Z'
        end_date = f'{year}-12-31T23:59:59.999999Z'
        df_tmp = Tardis().load(a=start_date, b=end_date, meta_file=r_meta_file)
        
        df_zorro = to_zorro_t6_compatible_csv(df_tmp)
        df_zorro.to_csv(os.path.join(zorro_csv_dir, f'binance_futures_{r_symbols[0]}_{year}.csv'), index=False)


# Run zorro:
# Zorro.exe -run CSVtoT6TardisData -u dir=C:\temp1 -u out=C:\temp2

if zorro_stay:
    sp.run(f'{zorro_exe} -stay -run CSVtoT6TardisData -u dir={zorro_csv_dir} -u out={zorro_t6_dir}')

else:
    sp.run(f'{zorro_exe} -run CSVtoT6TardisData -u dir={zorro_csv_dir} -u out={zorro_t6_dir}')

print ("\nFinito!")



