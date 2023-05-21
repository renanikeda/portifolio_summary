from module_summary import get_stock_list, get_dividend_table, treat_date, get_ceiling_price, get_mean_price, add_stocks, get_standard_deviation, get_sector, try_get_function, get_sector_distribution
from datetime import datetime, timedelta
import pandas as pd
pd.options.mode.chained_assignment = None

wallet_info_columns = ['stock', 'position', 'allocation', 'profit', 'price', 'last_price', 'quantity']
today = datetime.now() 

total_dividend_table = pd.DataFrame()
tracking_stocks = ['GGBR4', 'BBSE3']

wallet_info = get_stock_list()
wallet_info = add_stocks(wallet_info, tracking_stocks)
wallet_info = wallet_info[wallet_info['stock'] != "IVVB11"]
print(wallet_info)
stock_list = wallet_info['stock'].to_list()

#print(f"Stock List: {stock_list}")
    
stock_quantity = wallet_info[['stock', 'quantity', 'price']]
stock_quantity.columns = ['Acao', 'Quantidade', 'PM Compra']

writer = pd.ExcelWriter("Resumo Carteira.xlsx", engine = 'openpyxl', mode = 'a', date_format = "%d/%m/%Y", if_sheet_exists = 'overlay' )

for stock in stock_list:
    dividend_table = try_get_function(get_dividend_table, stock = stock, verbose = True)
    if(dividend_table is None):
        continue
    #time.sleep(1.5)
    dividend_table = dividend_table[dividend_table['Pagamento'].apply(treat_date) >= today]
    total_dividend_table = pd.concat([total_dividend_table, dividend_table.copy()], ignore_index=True)
    ceiling_price = get_ceiling_price(stock, 4, 0.06)
    mean_price = get_mean_price(stock, datetime.now().date() - timedelta(days=1))
    std_price = get_standard_deviation(stock, 1)
    index_row = stock_quantity[stock_quantity['Acao'] == stock].index
    stock_quantity.loc[index_row, 'Preco Teto'] = ceiling_price
    stock_quantity.loc[index_row, 'Std'] = round(std_price, 2)
    stock_quantity.loc[index_row, 'Preco Medio'] = mean_price
    setores = try_get_function(get_sector, stock = stock)
    stock_quantity.loc[index_row, 'Setor'] = setores.get('setor', '')
    stock_quantity.loc[index_row, 'Sub Setor'] = setores.get('subsetor', '')
    stock_quantity.loc[index_row, 'Segmento'] = setores.get('segmento', '')

stock_quantity['Total'] = stock_quantity['Quantidade'] * stock_quantity['Preco Medio']

#distribution = get_sector_distribution(stock_quantity)
stock_quantity.to_excel(writer, sheet_name="Quantidades", index = False)
#distribution.to_excel(writer, sheet_name="Dados", index = False)
total_dividend_table = total_dividend_table.join(stock_quantity.set_index('Acao')[['Quantidade']], on = "Acao")
total_dividend_table["A receber"] = total_dividend_table['Quantidade'] * total_dividend_table['Valor']
total_dividend_table.to_excel(writer, sheet_name = "Dividendos", index = False, float_format="%.2f")

print(stock_quantity)
print(total_dividend_table)
writer.close()
