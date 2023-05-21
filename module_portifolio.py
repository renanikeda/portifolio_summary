from utils import get_dividend_table, treat_date, today, get_ceiling_price_bazin, get_fair_price_graham
import pandas as pd
import os
import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

class Portifolio():
    def __init__(self, path: str) -> None:
        self.filename = path.split('/')[-1] if '/' in path else path
        self.path = '/'.join(path.split('/')[:-1]) if '/' in path else '.'
        self.position = None
        self.dividends_table = None
        self.portifolio = self.read_position()
        self.etfs = ['IVVB11']
        
    def read_position(self) -> None:
        self.columns = ['stock', 'position', 'allocation', 'profit', 'price', 'last_price', 'quantity']
        data = pd.read_excel(self.path + '/' + self.filename, engine='openpyxl', header=7)
        last_row = 0
        for row in data.iterrows():
            if row[1][0].strip() == '':
                last_row = row[0]
                break

        data = data[0:last_row]
        data.columns = self.columns
        data = data.applymap(lambda stock: stock.strip())
        data['quantity'] = data['quantity'].astype('int64')
        self.position = data[:1]
        return data

    def get_dividends(self) -> pd.core.frame.DataFrame:
        dividends_table = pd.DataFrame()
        for stock in self.position['stock']:
            if stock in self.etfs: continue
            dividends = get_dividend_table(stock, True)
            # dividends = dividends[dividends['Pagamento'].apply(treat_date) >= today]
            dividends_table = pd.concat([dividends_table, dividends.copy()], ignore_index=True)

        self.dividends_table = dividends_table
        return dividends_table[dividends_table['Pagamento'].apply(treat_date) >= today]
    
    def get_summary(self) -> None:
        position = self.position[['stock', 'quantity', 'price', 'position', 'allocation']]
        if self.dividends_table is None:
            self.get_dividends()
        for stock in position['stock']:
            dividend_table = self.dividends_table[self.dividends_table['Acao'] == stock]
            bazin_price = get_ceiling_price_bazin(dividend_table)
            print(stock, bazin_price)
            

    def save_excel(self, filename) -> None:
        file_already_exists = filename in os.listdir(self.path)
        mode = 'a' if file_already_exists else 'w'
        if_sheet_exists = 'replace' if file_already_exists else None 
        
        self.writer = pd.ExcelWriter(filename, engine = 'openpyxl', mode = mode, date_format = "%d/%m/%Y", if_sheet_exists = if_sheet_exists )

        self.portifolio.to_excel(self.writer, sheet_name = "teste", index = False, float_format="%.2f")

portifolio = Portifolio('C:/Users/renan/OneDrive/Documentos/PYTHON/portifolio_summary/PosicaoDetalhada.xlsx')
print(portifolio.get_summary())