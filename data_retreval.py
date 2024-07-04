import ccxt
import pandas as pd
import requests
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import requests
from bs4 import BeautifulSoup
import json

def get_all_tickers():
    exchange = ccxt.bybit()
    data = exchange.fetchTickers()
    #ticker = [ i for i in data if ((data[i]['quoteVolume'] >5000000) and  (i.split(':')[-1] == 'USDT')) ]
    ticker = [ i for i in data if (  (i.split(':')[-1] == 'USDT')) ]

    return ticker

def get_ohlcv(symbol, interval,mode ='all'):
    limit = 1000
    page = 10
    if mode == 'all':
        page = 100
    elif  mode == 'update':
        page = 1
        
    exchange = ccxt.bybit()

    ohlcv = exchange.fetch_ohlcv(symbol, interval, limit=limit)
    start_ts = ohlcv[0][0]
    end_ts = ohlcv[-1][0]
    gap = end_ts - start_ts
    
    d_list = []
    d_list +=ohlcv
    for i in range(page-1):
        start = start_ts - gap
        
        temp = exchange.fetch_ohlcv(symbol, interval, limit=limit ,since = start_ts - gap )
        start_ts = temp[0][0]
        d_list = temp +d_list


    
    df = pd.DataFrame(d_list, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df['Date'] = pd.to_datetime(df['Timestamp'], unit='ms')
    df['symbol'] = symbol
    df['interval'] = interval

    return df

def get_fr(symbol,mode ='all'):
    ex = ccxt.bybit()
    page = 10
    if mode == 'all':
        page = 100
    elif  mode == 'update':
        page = 1
        
    data  = ex.fetch_funding_rate_history(symbol, limit=200 , params = {"paginate": True, "paginationCalls": page})
    data =  [d['info'] for d in data]
    df = pd.DataFrame(data)
    df['symbol'] = symbol

    return df

def get_marketcap_detail_by_type(tag):
    api_key = 'coinrankinge8183a3f446ad8094d3c76f754dcfa85d0365b5ecf824cc3'
    headers = {
    'x-access-token': api_key,
    }

    response = requests.request("GET", f"https://api.coinranking.com/v2/coins?tags[]={tag}", headers=headers)
    data = response.json()
    total_market = total = data['data']['stats']['totalMarketCap']
    each = []

    t= []
    for i in data['data']['coins']:
        t.append( float(i['marketCap']))
    total =sum(t)

    for i in data['data']['coins']:
        t.append( float(i['marketCap']))
        each.append(
                    {
                        'symbol' : i['symbol'],
                        'marketcap' : i['marketCap'],
                        'all_mearket' : total_market,
                        'ratio' : float(i['marketCap']) / float(total),
                        'all_market' : total_market,
                        'all_marketratio' : float(i['marketCap']) / float(total_market),
                    }
                )

    print(sum(t))
    return total,each

def get_list_of_coin_by_type(tag):
    api_key = 'coinrankinge8183a3f446ad8094d3c76f754dcfa85d0365b5ecf824cc3'
    headers = {
    'x-access-token': api_key,
    }

    response = requests.request("GET", f"https://api.coinranking.com/v2/coins?tags[]={tag}", headers=headers)
    
    symbol = []
    for i in response.json()['data']['coins']:
        symbol.append(i['symbol'])
    return symbol


def get_list_of_type():
    return ['defi',
            'stablecoin',
            'nft',
            'dex',
            'exchange',
            'staking',
            'dao',
            'meme',
            'privacy',
            'metaverse',
            'gaming',
            'wrapped',
            'layer-1',
            'layer-2',
            'fan-token',
            'football-club',
            'web3',]


def get_list_of_type_cmc():
    cmc_api_key = 'ea04c239-b5ab-46c5-9dac-163acad950ef'

    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/categories'
    parameters = {
      'start':'1',
      'limit':'5000',
    }
    headers = {
      'Accepts': 'application/json',
      'X-CMC_PRO_API_KEY': cmc_api_key ,
    }

    session = Session()
    session.headers.update(headers)

    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        tags = [i['name'] for i in data['data']]

      
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        return e
    return tags

def get_type_cat_cmc():
    cmc_api_key = 'ea04c239-b5ab-46c5-9dac-163acad950ef'
    symbol_list  = [] 

    exception_list=  "10000LADYS,10000NFT,10000SATS,10000STARL,10000WEN,1000BONK,1000BTT,1000FLOKI,1000LUNC,1000PEPE,1000RATS,1000XEC,LUNA2,SHIB1000".split(',')

    for symbol in get_all_tickers():
        symbol_str = symbol.split('/')[0]
        symbol_list.append(symbol_str)

    url = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/info'
    parameters = {

      'symbol' : ','.join(symbol_list).replace('10','').replace('0','').replace('2','')
    }

    headers = {
      'Accepts': 'application/json',
      'X-CMC_PRO_API_KEY': cmc_api_key ,
    }

    session = Session()
    session.headers.update(headers)

    response = session.get(url, params=parameters)
    data = json.loads(response.text)['data']
    #print(len(data))
    #print(len(get_all_tickers()))

    #tags = [i['name'] for i in data['data']]

    tags = []
    for symbol,datum in zip(get_all_tickers(),data):
        #print(symbol)
        #print(data[datum][0]['id'])
        #print(data[datum][0]['category'])
        #print(data[datum][0]['tags'])

        tags.append(
            {
                'symbol' : symbol,
                'category': data[datum][0]['category'],
                'tags' : data[datum][0]['tags'],
            }
            )


    return tags

def get_rich_list(name):
    resopnse = requests.get(f'https://www.coinlore.com/coin/{name}/richlist')
    html = BeautifulSoup(resopnse.text,'lxml').find(id='topholdersdiv')
    df = pd.read_html(str(html))[0]
    return df.to_dict('records')

def get_all_chain_tvl():
    url = 'https://api.llama.fi/v2/chains'
    resopnse = requests.get(url).json()
    return pd.DataFrame(resopnse)[['tokenSymbol','name','tvl','cmcId']].to_dict('records')


if __name__ == '__main__':
    # print(get_fr('BTC/USDT:USDT',mode = 'update'))
    # print(get_marketcap_detail_by_type('defi'))
    # print(get_list_of_coin_by_type('defi'))
    print(get_all_chain_tvl())