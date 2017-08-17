import quandl
from os import path, makedirs
from pandas import read_csv
from requests import get
import zipfile
from io import BytesIO, StringIO
import credentials
import os

import path

######################################
### Generic parameters
######################################
quandl.ApiConfig.api_key = credentials.Quandl["authtoken"]
Months = {'F':1, 'G':2, 'H':3, 'J':4, 'K':5, 'M':6,'N':7, 'Q':8, 'U':9, 'V':10, 'X':11, 'Z':12}
FGHJKMNQUVXZ = {v: k for k, v in Months.items()}


######################################
### Functions
######################################

# Return a csv format string
def fetch(market, symbol, month, year):
    data = quandl.get(f'{market}/{symbol}{FGHJKMNQUVXZ[month]}{year}')# Datafrane by default
    if data.empty:
        return None
    else:
        #data = data[['Open','High','Low','Settle','Volume']] # for QuantConnect
        return data#.to_csv(date_format='%Y%m%d %H:%M',header=True) # set header false for QuantConnect

# Return a stream
def fetch_stream(market, symbol, month, year):
    '''
        https://docs.quandl.com/v1.0/docs/quick-start-examples-1
    '''
    request = f'{path.Quandl["datasets"]}{market}/{symbol}{FGHJKMNQUVXZ[month]}{year}.csv?api_key={quandl.ApiConfig.api_key}&order=asc'
    response = get(request)
    return response.content


# It does not return daily and weekly contract
def get_existing_contract(market):

    '''
    The contracts have not the same description convention. Hence relying on the quandl code is more easy.
    '''
    req = get(f'{path.Quandl["databases"]}/{market}/codes.csv')
    file = zipfile.ZipFile(BytesIO(req.content), mode='r')
    name_csv = file.filelist[0].filename
    cc = file.read(name=name_csv)
    existing_contract = read_csv(StringIO(cc.decode('utf-8')), header=None, usecols=[0,1], names=['QuandlCode','description'])

    existing_contract = existing_contract[existing_contract['description'].str.contains('Futures')==True]
    existing_contract = existing_contract[existing_contract['description'].str.contains('Daily|Weekly|Option')==False]

    existing_contract['year'] = existing_contract['QuandlCode'].apply(lambda s: s[-4:]) # some years are not int (see Rough Rice 20RR) so cast to int not possible
    existing_contract['month'] = existing_contract['QuandlCode'].apply(lambda s: s[-5:-4])
    existing_contract['symbol'] = existing_contract['QuandlCode'].apply(lambda s: s[4:-5])
    existing_contract['code'] = existing_contract['symbol']+ "_"  + existing_contract['year'] + existing_contract['month']
    existing_contract['month'] = existing_contract['month'].apply(lambda s: Months[s])
    # existing_contract['code'] = existing_contract['full_name'].apply(lambda s: re.search(r"\((.*?)\)",s).group(1))
    existing_contract['dataset'] = market

    return existing_contract



# contracts is a dataframe with a column year (int:2015) and a column month (str: '01' for Jan)
def _get_zip(market, symbol, contracts):
    directory = f'{market.lower()}/{symbol.lower()}'
    if not os.path.exists(directory):
        makedirs(directory)
    zip_name = f'{directory}/{symbol.lower()}.zip'
    with zipfile.ZipFile(zip_name, 'w') as zip:
        for i, contract in contracts.iterrows():
            file = fetch_stream(market, symbol, contract['month'], contract['year'])
            if not (file is None):
                print(contract)
                zip.writestr(f"{symbol.lower()}_{contract['year']}{FGHJKMNQUVXZ[contract['month']]}.csv", file)


# download all the contracts for a market. See main.py to learn how to use filter
def get_zip_all_data_from_symbol(market, filter):
    existing_contract = get_existing_contract(market)
    existing_contract = existing_contract[filter(existing_contract)]
    symbols = set(existing_contract['symbol'])
    print(f'There are {len(symbols)} symbols meeting the conditions in {market}')
    i = 1
    for symbol in symbols: # Concurrent call are prohibited by quandl
        print(f'Start processing {market}/{symbol}: {i}/{len(symbols)}')
        contracts = existing_contract[existing_contract['symbol']==symbol]
        _get_zip(market, symbol, contracts)
        i=i+1

