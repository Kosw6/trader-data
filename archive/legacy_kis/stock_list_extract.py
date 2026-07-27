import pandas as pd
import numpy as np
import mongo_crud

def save_stock_list(val) :
    df = pd.read_excel(f'./stock_list_excel/{val}_code.xlsx')
    selected_columns = ['Exchange code', 'Symbol', 'Korea name','English name','Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)','업종분류코드','구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)']
    df_selected = df[selected_columns]
    df_selected = df_selected.rename(columns={'Exchange code': 'excd', 'Symbol': 'symb','Korea name':'name','English name':'ename'
                                              ,'Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)':'stock_type','업종분류코드':'GICS','구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)':'classification_code'})
    df_selected = df_selected.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df_selected.replace('', np.nan, inplace=True)
    df_selected.fillna(0,inplace=True)

    mongo_crud.save_stock_list(df_selected.to_dict('records'))

cmd = input("시장의 코드를 입력하여 주세요. \nnas:나스닥, nys:뉴욕, ams:아멕스, shs:상해, shi:상해지수, szs:심천, szi:심천지수, tse:도쿄, hks:홍콩, hnx:하노이, hsx:호치민\n")

save_stock_list(cmd)
