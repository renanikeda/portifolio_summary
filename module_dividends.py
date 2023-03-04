from bs4 import BeautifulSoup
from random_user_agent.user_agent import UserAgent
from datetime import datetime, date, timedelta
from cachetools import cached, TTLCache, keys
import pandas as pd
import json
import requests
import urllib3
import random 
import time
import re

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
cache = TTLCache(maxsize=100, ttl=5*60)
stock_list = ["BBAS3", "BRAP4", "BRKM5", "CPLE6", "ITSA4", "PETR4", "TAEE11", "TRPL4", "USIM5"]
fii_list = []

def join_delimiter(arg_list, delimiter = '_'):
    if isinstance(arg_list[0], tuple) or isinstance(arg_list[0], list):
        return [delimiter.join(map(str, el)) for el in arg_list]
    else:
        return delimiter.join(map(str, arg_list))

def cache_key(*args, **kwargs):
    key_args, key_kwargs = ['', '']
    if args:
        key_args = join_delimiter(args)
    if kwargs:
        key_kwargs = join_delimiter(join_delimiter((sorted(kwargs.items()))))

    return key_args + '_' + key_kwargs if key_args and key_kwargs else key_args or key_kwargs
    
def is_empty(obj):
    if obj.__class__.__module__ == 'builtins':
        return not obj
    elif 'pandas' in  obj.__class__.__module__:
        return obj.empty
    else:
        raise Exception('Type not defined in is_empty function')
    
def try_get_function(func = lambda x: x, limit = 5, *args, **kargs):
    response = {}
    backoff = 1
    attempt = 0
    while (is_empty(response) and attempt < limit):
        if attempt > 0 : print(f"Attempt {attempt} in {func.__doc__.strip()}")
        time.sleep(attempt*backoff)
        response = func(*args, **kargs)
        attempt += 1
    return response

def add_stocks(wallet, stocks):
    wallet_cols = wallet.columns.to_list()
    new_stocks = pd.DataFrame([[stock, *[0]*(len(wallet_cols) - 1)] for stock in stocks], columns = wallet_cols)
    return pd.concat([wallet, new_stocks], ignore_index=True)

def get_stock_list(file = "PosicaoDetalhada.xlsx"):
    data = pd.read_excel(file, engine='openpyxl', header=7)
    
    last_row = 0
    for row in data.iterrows():
        #print(row[1][0])
        if row[1][0].strip() == '':
            last_row = row[0]
            break
    data = data[0:last_row+1]
    data.columns = ['stock', 'position', 'allocation', 'profit', 'price', 'last_price', 'quantity']
    data = data[data['stock'].apply(lambda stock: stock.strip()) != '']
    data['quantity'] = data['quantity'].apply(lambda quantity: int(quantity))
    data = data[data['quantity'].apply(lambda quantity: quantity) > 0]
    return data

def treat_numbers(value):
    if isinstance(value, str):
        return float((re.sub('\s*[A-za-z]\s*', '', value)).replace(',','.'))
    if isinstance(value, int): 
        return float(value)
    return value

def treat_date(data, format = "%d/%m/%Y", remove_time = True):
    try:
        return datetime.strptime(data, format)
    except:
        return date(1900,1,1)

def try_get_dividend_table(acao = "PETR4", limit = 5, verbose = False):
    dividend_table = pd.DataFrame()
    backoff = 1
    attempt = 0
    while (dividend_table.empty and attempt < limit):
        if attempt > 0 : print(f"Attempt {attempt} to get dividend table {acao}")
        time.sleep(attempt*backoff)
        dividend_table = get_dividend_table(acao, verbose)
        attempt += 1
    return dividend_table

def try_get_stock_price_series(stock = "PETR4", limit = 5):
    prices_df = pd.DataFrame()
    backoff = 1
    attempt = 0
    while (prices_df.empty and attempt < limit):
        if attempt > 0 : print(f"Attempt {attempt} to get stock price series {stock}")
        time.sleep(attempt*backoff)
        prices_df = get_stock_price_series(stock)
        attempt += 1
    return prices_df

@cached(cache, key = lambda *args, **kwargs: cache_key('get_stock_price_series', *args, **kwargs))
def get_stock_price_series(stock = 'ITSA4'):
    """
    Get Stock Price Function
    """
    try:
        headers = generate_header()
        response = requests.post('https://statusinvest.com.br/acao/tickerprice', data = { 'ticker': stock, 'type': 4 }, headers = headers)
        resp_obj = json.loads(response.text)
        prices_df = pd.DataFrame(resp_obj[0].get('prices', ''))
        prices_df['date'] = prices_df['date'].apply(lambda date: treat_date(date, "%d/%m/%y %H:%M").date())
        return prices_df
    except Exception as err:
        print(f"Couldn't get stock price. Trying again. Error: {err}")
        cache.clear()
        return pd.DataFrame()

def get_mean_price(stock, data_ref):
    price_series = try_get_function(get_stock_price_series, stock = stock)
    price_series = price_series[(price_series['date'] == data_ref)]
    return price_series['price'].mean()

def get_standard_deviation(stock, years = 5):
    try:
        now = datetime.now().date()
        price_series = try_get_function(get_stock_price_series, stock = stock)
        price_series = price_series[(price_series['date'] <= now)]
        price_series = price_series[(price_series['date'] >= (now - timedelta(days=1*365)))]
        return price_series['price'].std()
    except Exception as err:
        print(f"Couldn't get standard deviation. Err: {err}")
        return 0
    
@cached(cache, key = lambda *args, **kwargs: cache_key('get_dividend_table', *args, **kwargs))
def get_dividend_table(stock = 'PETR4', verbose = False):
    """
    Get Dividend Table Function
    """
    try:
        headers = generate_header()
        response = requests.get(f'https://statusinvest.com.br/acao/companytickerprovents?ticker={stock}&chartProventsType=2', timeout = 3, verify = True, headers = headers)
        json_obj = json.loads(response.text)
        dividend_df = pd.DataFrame(json_obj.get('assetEarningsModels', {}))
        try:
            dividend_df.drop(['y', 'm', 'd', 'etd', 'sv', 'sov', 'adj'], axis = 1, inplace = True)
            dividend_df.rename(columns={"ed": "DATA COM", "pd": "Pagamento", 'v': 'Valor', 'et': 'Tipo'}, inplace = True)
            dividend_df.insert(0, 'Acao', stock)
            dividend_df = dividend_df[['Acao', 'Tipo', 'DATA COM', 'Pagamento', 'Valor']]
            if verbose: print(f'{stock} Table Found!')
        except:
            print(f'It was not possible to rearrange dataframe or found {stock}')
        return dividend_df
    
    except Exception as err:
        print("Couldn't get table. Trying again.")
        cache.clear()
        return pd.DataFrame()

def get_ceiling_price(asset = 'ITSA4', years = 5, dy = 0.065):
    try:
        now = datetime.now()
        ceiling_date = date(now.year - 1, 12, 31).strftime("%Y-%m-%d")
        floor_date = date(now.year - years, 1, 1).strftime("%Y-%m-%d")
        dividend_table = try_get_function(get_dividend_table, stock = asset)
        dividend_table.replace('-', date(now.year + 1, 12, 31).strftime("%d/%m/%Y"), inplace = True)
        #dividend_table['Pagamento'] = pd.to_datetime(dividend_table['Pagamento'], format='%d/%m/%Y')
        #dividend_table['DATA COM'] = pd.to_datetime(dividend_table['DATA COM'], format='%d/%m/%Y')
        dividend_table['Pagamento'] = dividend_table['Pagamento'].apply(treat_date)
        dividend_table['DATA COM'] = dividend_table['DATA COM'].apply(treat_date)
        dividend_table = dividend_table[(dividend_table['Pagamento'] >= floor_date)]
        dividend_table = dividend_table[(dividend_table['Pagamento'] <= ceiling_date)]
        min_date = dividend_table['Pagamento'].min()
        max_date = dividend_table['Pagamento'].max()
        diff_years = (max_date.year - min_date.year + 1)
        min_years = years if years <= diff_years else diff_years
        ceiling_price = round((dividend_table['Valor'].sum()/min_years)/dy, 2)
        return ceiling_price
    except Exception as err:
        print(f"Couldn't get ceiling price. Err: {err}")
        return 0

def get_sector(stock = "ITSA4"):
    """
    Get Sector Function
    """
    try:
        headers = generate_header()
        response = requests.get(f'https://statusinvest.com.br/acoes/{stock}', timeout = 3, verify = True, headers = headers)
        soup = BeautifulSoup(response.text, "html.parser")

        setor = soup.find('span', text = "Setor de Atuação").find_parent().div.a.strong.text
        subsetor = soup.find('span', text = "Subsetor de Atuação").find_parent().div.a.strong.text
        segmento = soup.find('span', text = "Segmento de Atuação").find_parent().div.a.strong.text

        return { 'setor': setor, 'subsetor': subsetor, 'segmento': segmento }
    
    except Exception as err:
        print("Couldn't get Sector info. Error: ", err)
        return {}
    
def generate_header():
    headers = {
        'authority': 'statusinvest.com.br',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'user-agent': '',
    }
    #user_agent = random.choice(user_agents)  
    user_agent_rotator = UserAgent(limit=100)
    user_agent = user_agent_rotator.get_random_user_agent()
    headers['user-agent'] = user_agent
    return headers

user_agents = [ 
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36', 
	'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36', 
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36', 
	'Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148', 
	'Mozilla/5.0 (Linux; Android 11; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Mobile Safari/537.36' ,
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Linux; Android 10; SM-G996U Build/QP1A.190711.020; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 10; SM-G980F Build/QP1A.190711.020; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/78.0.3904.96 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 5.1.1; SM-G928X Build/LMY47X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.83 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 10; Google Pixel 4 Build/QD1A.190821.014.C2; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/78.0.3904.108 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 6.0; HTC One X10 Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/61.0.3163.98 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone13,2; U; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/15E148 Safari/602.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1',
    'Mozilla/5.0 (Apple-iPhone7C2/1202.466; U; CPU like Mac OS X; en) AppleWebKit/420+ (KHTML, like Gecko) Version/3.0 Mobile/1A543 Safari/419.3',
    'Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 950) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Mobile Safari/537.36 Edge/13.1058',
    'Mozilla/5.0 (Linux; Android 6.0.1; SGP771 Build/32.2.A.0.253; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/52.0.2743.98 Safari/537.36',
    'Mozilla/5.0 (Linux; Android 4.4.3; KFTHWI Build/KTU84M) AppleWebKit/537.36 (KHTML, like Gecko) Silk/47.1.79 like Chrome/47.0.2526.80 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1'
] 
