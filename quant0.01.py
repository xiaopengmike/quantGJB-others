#5连扳后断板策略_问财接口

import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import calendar

MonthRange=range(1,11,1)
tradeDays=get_all_trade_days()

def Function1(MonthRange):
    dataframe=pd.DataFrame()
    for i in MonthRange:
        if i==1:
            startDate="2022-12-01"
            endDate="2023-2-28"
        else:
            startDate="2023-"+str(i-1)+"-01"
            endDate="2023-"+str(i+1)+"-"+str(calendar.monthrange(2023,i+1)[1])
        df=query_iwencai(str(i)+"月 >4板，非st")
        tradeRange=get_trade_days(startDate,endDate)
        for num in range(len(df['股票代码'])):
            ticker=df.loc[num,'股票代码']
            value=get_price(ticker, str(tradeRange[0]), str(tradeRange[-1]),'1d',['open','close','low','high','high_limit','quote_rate'],True,'pre',0,False)
            for m in range(len(value)-4):
                mIndex=value.index[m]
                t=0
                if value.loc[mIndex,'close']==value.loc[mIndex,'high_limit']:
                    n=m
                    nIndex=value.index[n]
                    while value.loc[nIndex,'close']==value.loc[nIndex,'high_limit']:
                            highestBreak=value.loc[nIndex,'high']
                            t=t+1
                            n=n+1
                            nIndex=value.index[n]
                            if n==range(len(value))[-1]:break
                    if t>4:
                        if highestBreak<value.loc[nIndex,'high']:
                            highestBreak=value.loc[nIndex,'high']
                        df.loc[num,'断板时间']=value.index[n]
                        df.loc[num,'断板前后最高价']=highestBreak
                        j=n
                        if j==range(len(value))[-1]:
                            df.loc[num,'断板回调到最低价的时间']=value.index[j]
                        else:
                            while value.loc[value.index[j+1],'low']<=value.loc[value.index[j],'low'] or value.loc[value.index[j+1],'low']/value.loc[value.index[j],'low']<=1.0002:
                                j=j+1
                                if j==range(len(value))[-1]:
                                    break
                            df.loc[num,'断板回调到最低价的时间']=value.index[j]
                            lowest=value.loc[value.index[j],'low']
                            df.loc[num,'回调最低价']=lowest
                            df.loc[num,'时间间隔']=j-n
                            df.loc[num,'回调百分比']=(lowest-highestBreak)/highestBreak
                            if j!=range(len(value))[-1] and j!=range(len(value))[-2]:
                                k=j+1
                                while value.loc[value.index[k],'high']<value.loc[value.index[k+1],'high'] or value.loc[value.index[k],'high']/value.loc[value.index[k+1],'high']<=1.0002:
                                    k=k+1
                                    if k==range(len(value))[-1]:
                                        break
                                highest=value.loc[value.index[k],'high']
                                df.loc[num,'第一次反弹的最高价']=highest
                                df.loc[num,'反弹的比率']=(highest-lowest)/lowest
                                df.loc[num,'第一次反弹的时间间隔']=k-j
                                df.loc[num,'第一次反弹结束时间']=value.index[k]
        dataframe=dataframe.append(df, ignore_index=True)
    return dataframe

    
    