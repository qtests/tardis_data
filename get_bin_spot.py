import os
import pandas as pd

import datetime as dt
import subprocess as sp

from TData_lib import to_zorro_t6_compatible_csv, rename_files, make_new_name, extended_in


all_years = [2019, 2020, 2021]

coin_subset = []

take_all_source_files = True

convert_Source_to_zoroCSV = False
convert_zoroCSV_to_t6 = convert_Source_to_zoroCSV
zorro_stay = False
rename_t6_files = convert_zoroCSV_to_t6


source_dir    = r'C:\co\adata\crypto_csv'
zorro_csv_dir = r'C:\Users\Divas\.tardis-cache\zorro'
zorro_t6_dir  = r'C:\Users\Divas\.tardis-cache\zorro_t6'
zorro_exe     = r'C:\co\zorro\Zorro2444\Zorro.exe'
zorro_script  = 'CSVtoT6TardisData'


if not os.path.exists(zorro_csv_dir):
    os.makedirs(zorro_csv_dir)

if not os.path.exists(zorro_t6_dir):
    os.makedirs(zorro_t6_dir)



print(f'\n\n{dt.datetime.now()}--------- Starting ---------\n')

for count, f in enumerate(os.listdir(source_dir)):

    if not take_all_source_files:
        if not extended_in(coin_subset, f):
            continue

    if convert_Source_to_zoroCSV:
        df = pd.read_csv( os.path.join(source_dir, f) )
        df_idx = pd.to_datetime(df[ ['Date', 'Time'] ].apply (lambda x: str(x[0]) + ' ' + str(x[1]), axis=1), format="%Y%m%d %H:%M:%S")
        df = df.set_index( df_idx )
    
        for year in all_years:
    
            print ( count, " ", "Converting Source -> Zorro CSV: ", year, " ", f )        
            
            start_date = dt.datetime.strptime(f'{year}-01-01T00:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ')
            end_date = dt.datetime.strptime(f'{year}-12-31T23:59:59.999999Z', '%Y-%m-%dT%H:%M:%S.%fZ')
            df_tmp = df.loc[start_date : end_date]
    
            df_zorro = to_zorro_t6_compatible_csv(df_tmp)
            df_zorro.to_csv(os.path.join(zorro_csv_dir, f'binance_spot_{f.split("-")[0]}_{year}.csv'), index=False)


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




    

