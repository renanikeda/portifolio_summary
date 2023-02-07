from bs4 import BeautifulSoup
from datetime import datetime, date
import pandas as pd
import json
import requests
import urllib3
import random 
import time
import re

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

stock_list = ["BBAS3", "BRAP4", "BRKM5", "CPLE6", "ITSA4", "PETR4", "TAEE11", "TRPL4", "USIM5"]
fii_list = []

def get_stock_list(file = "PosicaoDetalhada.xlsx"):
    data = pd.read_excel(file, engine='openpyxl', header=7)
    
    last_row = 0
    for row in data.iterrows():
        print(row[1][0])
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

def treat_data(data):
    try:
        return datetime.strptime(data, "%d/%m/%Y")
    except:
        return date(1900,1,1)

def try_get_dividend_table(acao = "PETR4", limit = 5):
    dividend_table = pd.DataFrame()
    backoff = 1
    attempt = 1
    while (dividend_table.empty and attempt < limit):
        if attempt > 1 : print(f"Tentativa {attempt}")
        time.sleep(attempt*backoff)
        dividend_table = get_dividend_table(acao)
        attempt += 1
    return dividend_table

def get_dividend_table(acao = 'PETR4'):
    try:
        headers = {
            'authority': 'statusinvest.com.br',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'dnt': '1',
            'sec-ch-ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
            'sec-ch-ua-platform': '"Windows"',
            'upgrade-insecure-requests': '1',
            'user-agent': '',
        }
        user_agent = random.choice(user_agents) 
        headers['user-agent'] = user_agent
        response = requests.get(f'https://statusinvest.com.br/acoes/{acao}', timeout = 3, verify = True, headers = headers)
        soup = BeautifulSoup(response.text, "html.parser")

        tables = soup.find_all('table')
        dividend_table = None

        for table in tables:
            if table.th.attrs.get("title") == 'Tipo do provento': 
                dividend_table = table
                print(f"{acao} Dividend Table Found")
                continue
            
        dividend_df = pd.read_html(str(dividend_table), decimal=',', thousands='.')[0]
        dividend_df.insert(0, 'Acao', acao)
        dividend_df['Valor'] = dividend_df['Valor'].apply(treat_numbers)
        #dividend_df['Pagamento'] = dividend_df['Pagamento'].apply(lambda pagamento: datetime.strptime(pagamento, "%d/%m/%Y"))
        #dividend_df['DATA COM'] = dividend_df['DATA COM'].apply(lambda pagamento: datetime.strptime(pagamento, "%d/%m/%Y"))
        return dividend_df
    
    except Exception as err:
        print("Couldn't get table. Error: ", err)
        return pd.DataFrame()

def get_dividend_table_extended(acao = 'PETR4'):
    try:
        headers = {
            'authority': 'statusinvest.com.br',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'dnt': '1',
            'sec-ch-ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
            'sec-ch-ua-platform': '"Windows"',
            'upgrade-insecure-requests': '1',
            'user-agent': '',
        }
        user_agent = random.choice(user_agents)  
        headers['user-agent'] = user_agent
        response = requests.get(f'https://statusinvest.com.br/acao/companytickerprovents?ticker={acao}&chartProventsType=2', timeout = 3, verify = True, headers = headers)
        json_obj = json.loads(response.text)
        dividend_df = pd.DataFrame(json_obj.get('assetEarningsModels', {}))
        try:
            dividend_df.drop(['y', 'm', 'd', 'etd', 'sv', 'sov', 'adj'], axis = 1, inplace = True)
            dividend_df.rename(columns={"ed": "DATA COM", "pd": "Pagamento", 'v': 'Valor', 'et': 'Tipo'}, inplace = True)
            dividend_df.insert(0, 'Acao', acao)
            dividend_df = dividend_df[['Acao', 'Tipo', 'DATA COM', 'Pagamento', 'Valor']]
        except:
            print('It was not possible to rearrange dataframe')
        return dividend_df
    
    except Exception as err:
        print("Couldn't get table. Error: ", err)
        return pd.DataFrame()

def get_ceiling_price(asset = 'ITSA4', years = 5, dy = 0.08):
    now = datetime.now()
    ceiling_date = date(now.year - 1, 12, 31).strftime("%Y-%m-%d")
    floor_date = date(now.year - years, 1, 1).strftime("%Y-%m-%d")
    dividend_table = get_dividend_table_extended(asset)
    dividend_table['Pagamento'] = pd.to_datetime(dividend_table['Pagamento'], format='%d/%m/%Y')
    dividend_table['DATA COM'] = pd.to_datetime(dividend_table['DATA COM'], format='%d/%m/%Y')
    dividend_table = dividend_table[(dividend_table['Pagamento'] >= floor_date)]
    dividend_table = dividend_table[(dividend_table['Pagamento'] <= ceiling_date)]
    print(dividend_table)
    ceiling_price = (dividend_table['Valor'].sum()/years)/dy
    return ceiling_price


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

print(get_ceiling_price())