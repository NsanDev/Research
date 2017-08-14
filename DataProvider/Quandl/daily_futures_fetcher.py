import quandl
from os import path, makedirs
from pandas import read_csv
from logging import info
from requests import get
import zipfile
from io import BytesIO, StringIO
import credentials
import path
######################################
### Generic parameters
######################################
quandl.ApiConfig.api_key = credentials.Quandl["authtoken"]
Months = {'F':'01','G':'02','H':'03','J':'04','K':'05','M':'06'
            ,'N':'07','Q':'08','U':'09','V':'10','X':'11','Z':'12'}
tick_type = 'trade'

######################################
### Functions
######################################

## TODO: Optimize so that it dld only the columns we need
def fetch(market, symbol, month, year):
    data = quandl.Dataset(market+'/'+symbol+month+year).data().to_pandas()
    if data.empty:
        return None
    else:
        data = data[['Open','High','Low','Settle','Volume']]
        return data.to_csv(date_format='%Y%m%d %H:%M',header=False)

## This function will overwrite the existing zip

def get_zip(market, symbol, months, years, symbol_id):
    directory = '{}/{}'.format(market.lower(), symbol_id.lower())
    if not path.exists(directory):
        makedirs(directory)
    zip_name = directory+'/{}_{}.zip'.format(symbol_id.lower(), tick_type)
    with zipfile.ZipFile(zip_name, 'w') as zip:
        for year in years:
            for m in months:
                file = fetch(market, symbol, m, year)
                if not(file is None):
                    zip.writestr("{}_{}_{}{}.csv".format(symbol_id.lower(), tick_type, year, Months[m]), file)

# It does not return daily and weekly contract
def get_existing_contract(market):
    req = get('{}/{}/codes.json'.format(path.Quandl["api"],market))
    file = zipfile.ZipFile(BytesIO(req.content), mode='r')
    name_csv = file.filelist[0].filename
    cc = file.read(name=name_csv)
    existing_contract = read_csv(StringIO(cc.decode('utf-8')), header=None, usecols=[0,1], names=['symbol','full_name'])
    existing_contract = existing_contract[existing_contract['full_name'].str.contains('Futures')==True]
    existing_contract = existing_contract[existing_contract['full_name'].str.contains('Daily|Weekly|Option')==False]
    existing_contract['year'] = existing_contract['symbol'].apply(lambda s: s[-4:])
    existing_contract['month'] = existing_contract['symbol'].apply(lambda s: s[-5:-4])
    existing_contract['symbol'] = existing_contract['symbol'].apply(lambda s: s[4:-5])
    return existing_contract

# contracts is a dataframe with a column year and a column month
def get_zip(market, symbol, contracts, symbol_id):
    directory = '{}/{}'.format(market.lower(), symbol_id.lower())
    if not path.exists(directory):
        makedirs(directory)
    zip_name = directory + '/{}_{}.zip'.format(symbol_id.lower(), tick_type)
    with zipfile.ZipFile(zip_name, 'w') as zip:
        for i, contract in contracts.iterrows():
            file = fetch(market, symbol, contract['month'], contract['year'])
            if not (file is None):
                print(contract)
                zip.writestr("{}_{}_{}{}.csv"
                             .format(symbol_id.lower(), tick_type, contract['year'], Months[contract['month']]), file)

def get_zip_all_data_from_symbol(market, start_year=2016):
    existing_contract = get_existing_contract(market)
    existing_contract = existing_contract[existing_contract['year']>=str(start_year)]
    symbols = set(existing_contract['symbol'])
    print('There are {} symbol in {} (no daily and no weekly)'.format(len(symbols),market))
    i = 1
    for symbol in symbols:
        print('Start processing {}/{}: {}/{}'.format(market, symbol, str(i), str(len(symbols))))
        contracts = existing_contract[existing_contract['symbol']==symbol]
        get_zip(market, symbol, contracts, symbol)
        i=i+1