from DataProvider.Quandl.daily_futures_fetcher import get_zip_all_data_from_symbol

month = 'Z'

market = 'CME'
symbol = 'CY'
limit_exp='2019'


#contracts = get_existing_contract(market)
filter = lambda contracts: (contracts["symbol"]==symbol) & (contracts["year"]>=limit_exp) #contracts is a dataframe. Set filter to True if you want to download all contracts
get_zip_all_data_from_symbol(market,filter) # download all available future from market


#data = quandl.get("CME/CLU2017", authtoken=Quandl["authtoken"]).to_csv()

b=1
