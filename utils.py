from random_user_agent.user_agent import UserAgent
from datetime import datetime, date, timedelta
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import json
import time
import re

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

def is_empty(obj):
    if obj.__class__.__module__ == 'builtins':
        return not obj
    elif 'pandas' in  obj.__class__.__module__:
        return obj.empty
    else:
        raise Exception('Type not defined in is_empty function')
    
def try_get_function(func: callable):
    def wrapper(*args, **kargs):
        limit = 5
        response = {}
        backoff = 2
        attempt = 1
        while (is_empty(response) and attempt <= limit):
            if attempt > 1 : print(f"Attempt {attempt} in {func.__doc__.strip()}")
            time.sleep(attempt*backoff)
            response = func(*args, **kargs)
            attempt += 1
        return response
    return wrapper

def join_delimiter(arg_list, delimiter = '_'):
    if isinstance(arg_list[0], tuple) or isinstance(arg_list[0], list):
        return [delimiter.join(map(str, el)) for el in arg_list]
    else:
        return delimiter.join(map(str, arg_list))

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

@try_get_function
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
        return pd.DataFrame()

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

def get_ceiling_price_bazin(asset = 'ITSA4', years = 5, dividend_yield = 7/100):
    try:
        now = datetime.now()
        ceiling_date = date(now.year, 1, 1).strftime("%Y-%m-%d")
        floor_date = date(now.year - years, 1, 1).strftime("%Y-%m-%d")
        dividend_table = get_dividend_table(asset)
        dividend_table.replace('-', date(now.year + 1, 1, 1).strftime("%d/%m/%Y"), inplace = True)
        dividend_table['Pagamento'] = dividend_table['Pagamento'].apply(treat_date)
        #dividend_table['DATA COM'] = dividend_table['DATA COM'].apply(treat_date)
        dividend_table = dividend_table[(dividend_table['Pagamento'] >= floor_date)]
        dividend_table = dividend_table[(dividend_table['Pagamento'] <= ceiling_date)]
        min_date = dividend_table['Pagamento'].min()
        max_date = dividend_table['Pagamento'].max()
        diff_years = (max_date.year - min_date.year + 1)
        min_years = years if years <= diff_years else diff_years
        ceiling_price = round((dividend_table['Valor'].sum()/min_years)/dividend_yield, 2)
        return ceiling_price
    
    except Exception as err:
        print(f"Couldn't get ceiling price. Err: {err}")
        return 0

def get_fair_price_graham(asset = 'ITSA4'):
    headers = generate_header()
    response = requests.get(f'https://statusinvest.com.br/acoes/{asset}', timeout = 3, verify = True, headers = headers)
    soup = BeautifulSoup(response.text, "html.parser")
    LPA = treat_numbers(soup.find('h3', text = "LPA").find_parent().find_parent().div.strong.text)
    VPA = treat_numbers(soup.find('h3', text = "VPA").find_parent().find_parent().div.strong.text)
    GRAHAM_CTE = 22.5
    return np.sqrt(GRAHAM_CTE*LPA*VPA)