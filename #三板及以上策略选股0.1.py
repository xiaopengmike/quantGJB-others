#三板及以上策略选股0.1
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import calendar

MonthRange=range(10,12,1)
tradeDays=get_all_trade_days()

def SanBanCeLueXuanGu(MonthRange):
    dataframe=pd.DataFrame()
    dframe=pd.DataFrame()        
    tradeDays=get_trade_days("2023-"+str(MonthRange[0])+"-01","2023-"+str(MonthRange[-1])+"-"+str(calendar.monthrange(2023,MonthRange[-1])[1]))
    for i in MonthRange:
        startDate="2023-"+str(i-1)+"-01"
        endDate="2023-"+str(i+1)+"-"+str(calendar.monthrange(2023,i+1)[1])
        df=query_iwencai(str(i)+"月 >2连板，非st")
        tradeRange=get_trade_days(startDate,endDate)
        for num in range(len(df['股票代码'])):
            ticker=df.loc[num,'股票代码']
            days=df.loc[num,'连续涨停天数']
            value=get_price(ticker, str(tradeRange[0]), str(tradeRange[-1]),'1d',['open','close','low','high','high_limit','quote_rate','turnover_rate','prev_close','amp_rate'],True,None,0,False)
            for m in range(len(value)-days):
                mIndex=value.index[m]
                t=0
                dateList=[]
                if value.loc[mIndex,'close']==value.loc[mIndex,'high_limit']:
                    n=m
                    nIndex=value.index[n]
                    while value.loc[nIndex,'close']==value.loc[nIndex,'high_limit']:
                            highestBreak=value.loc[nIndex,'high']
                            t=t+1
                            n=n+1
                            nIndex=value.index[n]
                            if n==range(len(value))[-1]:break
                    if t>=days and (nIndex==tradeDays).sum()==0:
                        continue
                    if t>=days:
                        if highestBreak<value.loc[nIndex,'high']:
                            highestBreak=value.loc[nIndex,'high']
                        df.loc[num,'断板时间']=value.index[n]
                        df.loc[num,'断板前后最高价']=highestBreak
                        j=n
                        if j==range(len(value))[-1]:
                            df.loc[num,'断板回调到最低价的时间']=value.index[j]
                        else:
                            while value.loc[value.index[j+1],'low']<=value.loc[value.index[j],'low'] or value.loc[value.index[j+1],'low']/value.loc[value.index[j],'low']<=1.002:
                                j=j+1
                                if j==range(len(value))[-1]:
                                    break
                            df.loc[num,'断板回调到最低价的时间']=value.index[j]
                            lowest=value.loc[value.index[j],'low']
                            df.loc[num,'回调最低价']=lowest
                            df.loc[num,'回调时间']=j-n+1
                            df.loc[num,'回调百分比']=(lowest-highestBreak)/highestBreak
                            if j!=range(len(value))[-1] and j!=range(len(value))[-2]:
                                k=j+1
                                while value.loc[value.index[k],'high']<value.loc[value.index[k+1],'high'] or value.loc[value.index[k],'high']/value.loc[value.index[k+1],'high']<=1.002:
                                    k=k+1
                                    if k==range(len(value))[-1]:
                                        break
                                highest=value.loc[value.index[k],'high']
                                df.loc[num,'第一次反弹的最高价']=highest
                                df.loc[num,'反弹的比率']=(highest-lowest)/lowest
                                df.loc[num,'第一次反弹的时间间隔']=k-j
                                df.loc[num,'第一次反弹结束时间']=value.index[k]
                            else:
                                if j==range(len(value))[-2]:
                                    k=j+1
                                    highest=value.loc[value.index[k],'high']
                                    df.loc[num,'第一次反弹的最高价']=highest
                                    df.loc[num,'反弹的比率']=(highest-lowest)/lowest
                                    df.loc[num,'第一次反弹的时间间隔']=k-j
                                    df.loc[num,'第一次反弹结束时间']=value.index[k]
                            if n==j:
                                if value.loc[nIndex,'amp_rate']>5 and value.loc[nIndex,'low']/value.loc[nIndex,'prev_close']<=0.92:
                                    dateList.append(nIndex)
                            elif j<=n+3 and j>n:
                                for k in range(n,j+1,1):
                                    if value.loc[value.index[k],'amp_rate']>5 and value.loc[value.index[k],'low']/value.loc[value.index[k],'prev_close']<=0.92:
                                        if value.loc[value.index[k],'turnover_rate']/value.loc[nIndex,'turnover_rate']>=0.5:
                                            dateList.append(value.index[k])
                            elif j>n+3:
                                for k in range(n,n+4,1):
                                    if value.loc[value.index[k],'amp_rate']>5 and value.loc[value.index[k],'low']/value.loc[value.index[k],'prev_close']<=0.92:
                                        if value.loc[value.index[k],'turnover_rate']/value.loc[nIndex,'turnover_rate']>=0.5:
                                            dateList.append(value.index[k])
                            for n in range(len(dateList)):
                                df.loc[num,'符合条件的日期'+str(n+1)]=dateList[n]
                                    
        dataframe=dataframe.append(df, ignore_index=True)
    return dataframe
