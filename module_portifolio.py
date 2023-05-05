from utils import generate_header, try_get_function, get_ceiling_price_bazin, get_fair_price_graham
from typing import TypeVar
import pandas as pd
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

portifolio = Portifolio('C:/Users/renan/OneDrive/Documentos/PYTHON/portifolio_summary/PosicaoDetalhada.xlsx')
#print(portifolio.portifolio)
#print(portifolio.get_dividend_table())
print(get_fair_price_graham())
