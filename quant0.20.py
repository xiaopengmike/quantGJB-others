import pandas as pd
import numpy as np
import requests
import datetime
from datetime import datetime

data=pd.read_excel('C:\\Users\\mimaa\\OneDrive - The Chinese University of Hong Kong\\Desktop\\stocks.xlsx',sheet_name=0)
symbol=data['symbol']
def DataOfLookUp():
    df=pd.DataFrame()
    time=(datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
    df.loc[0,'日期时间']=time
    for ticker in symbol:
        tickerNumber=ticker[:6]
        tickerLocation=ticker[-2:]
        x=requests.get('https://eminterservice.eastmoney.com/UserData/GetWebTape?code='+tickerLocation+tickerNumber)
        text=x.text
        index=text.find('TapeZ')
        if index==-1:
            df[ticker]=None
        else:
            percent=text[41:47]
            index1=percent.find(',')
            df[ticker]=percent[:index1]
    return df
df=DataOfLookUp()
df.to_excel('东方财富爬虫数据.xlsx',sheet_name='sheet1')