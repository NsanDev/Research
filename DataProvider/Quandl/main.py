from DataProvider.Quandl.daily_futures_fetcher import get_zip, Months
from credentials import Quandl
month = 'Z'
years = ['2016', '2017', '2018', '2019', '2020', '2021']
market = 'ICE'
symbol = 'B'

import quandl

get_zip(market, symbol, Months, years, 'CL')

data = quandl.get("ICE/BN2017", authtoken=Quandl["authtoken"])
