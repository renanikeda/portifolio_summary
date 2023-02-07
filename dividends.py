from module_dividends import get_stock_list, try_get_dividend_table, treat_data, get_ceiling_price
from datetime import datetime
import pandas as pd
import time

today = datetime.now() 

total_dividend_table = pd.DataFrame()

wallet_info = get_stock_list()
print(wallet_info)
stock_list = wallet_info['stock'].to_list()
print(f"Stock List: {stock_list}")
    
stock_quantity = wallet_info[['stock', 'quantity']]
stock_quantity.columns = ['Acao', 'Quantidade']
stock_quantity['Preco Teto'] = ''
writer = pd.ExcelWriter("proximos_dividendos.xlsx", engine = 'openpyxl', mode = 'a', date_format = "%d/%m/%Y", if_sheet_exists = 'overlay' )

for stock in stock_list:
    dividend_table = try_get_dividend_table(stock)
    if(dividend_table is None):
        continue
    #time.sleep(1.5)
    dividend_table = dividend_table[dividend_table['Pagamento'].apply(treat_data) >= today]
    total_dividend_table = pd.concat([total_dividend_table, dividend_table.copy()])

    ceiling_price = get_ceiling_price(stock)
    index_row = stock_quantity[stock_quantity['Acao'] == stock].index
    stock_quantity.loc[index_row, 'Preco Teto'] = ceiling_price

stock_quantity.to_excel(writer, sheet_name="Quantidades", index = False)

total_dividend_table = total_dividend_table.join(stock_quantity.set_index('Acao'), on = "Acao")
total_dividend_table["A receber"] = total_dividend_table['Quantidade'] * total_dividend_table['Valor']
total_dividend_table.to_excel(writer, sheet_name = "Dividendos", index = False, float_format="%.2f")

print(total_dividend_table)
writer.save()
