import sys; 
print('Python %s on %s' % (sys.version, sys.platform))
# Python 3.9.10 (tags/v3.9.10:f2f3f53, Jan 17 2022, 15:14:21) [MSC v.1929 64 bit (AMD64)] on win32
sys.path.extend(['../crypto_trading'])


import os
import asyncio
import pandas as pd

import logging
from pathlib import Path

from market.tardis import Tardis, to_zorro_t6_compatible_csv, instrument_info

from TData_lib import download_year, months_in_year, rename_files, make_new_name

import datetime as dt
import subprocess as sp


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
zorro_script = 'CSVtoT6TardisData'


run_data_download = False
convert_Tardis_to_zoroCSV = run_data_download
convert_zoroCSV_to_t6 = convert_Tardis_to_zoroCSV
zorro_stay = False
rename_t6_files = convert_zoroCSV_to_t6


if not os.path.exists(zorro_csv_dir):
    os.makedirs(zorro_csv_dir)

if not os.path.exists(zorro_t6_dir):
    os.makedirs(zorro_t6_dir)


# binance = instrument_info("binance-futures")
# b_instruments = pd.DataFrame([
#         instr for instr in binance if instr["datasetId"].endswith("USDT")
#     ]).set_index("id").sort_index()
# print (b_instruments)


print(f'\n\n{dt.datetime.now()}--------- Starting ---------\n')

for year in all_years:

    # Data download
    if run_data_download:
        for symb in all_symbols:
            asyncio.run(download_year(symbol=symb, dyear=year, dmonths=months_in_year(year)))
        
    # New content
    new_content = Tardis().stored_content()

    if len(new_content) > 0 and convert_Tardis_to_zoroCSV:
        new_df = pd.DataFrame(new_content)
        for idx, arow in new_df[['meta_file', 'symbols']].iterrows():
            r_meta_file, r_symbols = arow[0], arow[1]
    
            print ( "Converting Tardis -> Zorro CSV: ", r_symbols[0], " ", year, " ", r_meta_file )
            
            start_date = f'{year}-01-01T00:00:00.000Z'
            end_date = f'{year}-12-31T23:59:59.999999Z'
            df_tmp = Tardis().load(a=start_date, b=end_date, meta_file=r_meta_file)
            
            df_zorro = to_zorro_t6_compatible_csv(df_tmp)
            df_zorro.to_csv(os.path.join(zorro_csv_dir, f'binance_futures_{r_symbols[0]}_{year}.csv'), index=False)


# Run zorro:
# Zorro.exe -run CSVtoT6TardisData -u dir=C:\temp1 -u out=C:\temp2

if convert_zoroCSV_to_t6:
    print ("\nConverting Zorro CSV -> t6")
    
    if zorro_stay:
        sp.run(f'{zorro_exe} -stay -run {zorro_script} -u dir={zorro_csv_dir} -u out={zorro_t6_dir}')
    else:
        sp.run(f'{zorro_exe} -run {zorro_script} -u dir={zorro_csv_dir} -u out={zorro_t6_dir}')


# Rename files
if rename_t6_files:
    print ("\nRanaming t6 files")
    rename_files(zorro_t6_dir, f_new_name=make_new_name)


print ("\nFinito!")



