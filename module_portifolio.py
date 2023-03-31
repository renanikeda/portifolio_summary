from bs4 import BeautifulSoup
from typing import TypeVar
from datetime import datetime, date, timedelta
from utils import generate_header, try_get_function
import pandas as pd
import numpy as np
import json
import requests
import os
import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

class Portifolio():
    def __init__(self, path: str) -> None:
        self.filename = path.split('/')[-1] if '/' in path else path
        self.path = '/'.join(path.split('/')[:-1]) if '/' in path else '.'
                
        self.portifolio = self.read_position()
        
    def read_position(self) -> None:
        self.columns = ['stock', 'position', 'allocation', 'profit', 'price', 'last_price', 'quantity']
        data = pd.read_excel(self.path + '/' + self.filename, engine='openpyxl', header=7)
        last_row = 0
        for row in data.iterrows():
            #print(row[1][0])
            if row[1][0].strip() == '':
                last_row = row[0]
                break
        data = data[0:last_row]
        data.columns = self.columns
        data = data.applymap(lambda stock: stock.strip())
        data['quantity'] = data['quantity'].astype('int64')
        return data

    def save_excel(self, filename) -> None:
        file_already_exists = filename in os.listdir(self.path)
        mode = 'a' if file_already_exists else 'w'
        if_sheet_exists = 'replace' if file_already_exists else None 
        
        self.writer = pd.ExcelWriter(filename, engine = 'openpyxl', mode = mode, date_format = "%d/%m/%Y", if_sheet_exists = if_sheet_exists )

        self.portifolio.to_excel(self.writer, sheet_name = "teste", index = False, float_format="%.2f")

    @try_get_function
    def get_dividend_table(self, stock = 'PETR4', verbose = False):
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



portifolio = Portifolio('C:/Users/renan/OneDrive/Documentos/PYTHON/portifolio_summary/PosicaoDetalhada.xlsx')
print(portifolio.portifolio)
print(portifolio.get_dividend_table())
