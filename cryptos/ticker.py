import requests
import pandas as pd
from secrets import API_KEY


symbols = ['BTC', 'ETH', 'XRP', 'LTC']


def get(url, payload, symbol):
    payload['symbol'] = symbol
    r = requests.get(url, params=payload)
    return r.json()


def dict_to_df(symbol, d):

    metadata, data = d.values()
    df = pd.DataFrame.from_dict(data, orient='index')
    df['symbol'] = symbol

    return df


def dataframify(results):

    dfs = [dict_to_df(symbol, vals) for symbol, vals in results.items()]
    df = pd.concat(dfs)

    return df


def pull_prices(symbols, market='USD', apikey=API_KEY):

    url = 'https://www.alphavantage.co/query?'
    payload = {'function': 'DIGITAL_CURRENCY_INTRADAY',
               'market': market,
               'apikey': apikey}

    results = {symbol: get(url, payload, symbol) for symbol in symbols}

    cols = {f'1a. price ({market})': f'price_{market.lower()}',
            '1b. price (USD)': 'price_usd',
            '2. volume': 'volume',
            '3. market cap (USD)': 'market_cap_usd'}

    df = dataframify(results)
    df = format_data(df, cols)
    return df


def is_usd_duplicated(cols):
    return len([c for c in cols if 'USD' in cols]) > 1

def format_data(df, cols):
    if is_usd_duplicated(df.columns): # don't duplicate column
        df = df.drop('1a. price (USD)', axis=1)

    df = df.rename(columns=cols)

    return df
