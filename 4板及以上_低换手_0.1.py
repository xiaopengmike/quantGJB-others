#5连扳后断板策略_问财接口

import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import calendar

MonthRange=range(1,11,1)
tradeDays=get_all_trade_days()

def LowTurnoverRate(MonthRange):
    dataframe=pd.DataFrame()
    turnoverRate=8
    numOfLowTurnOver=2
    for i in MonthRange:
        if i==1:
            startDate="2022-12-01"
            endDate="2023-2-28"
        else:
            startDate="2023-"+str(i-1)+"-01"
            endDate="2023-"+str(i+1)+"-"+str(calendar.monthrange(2023,i+1)[1])
        #当所需要的数据范围不为2023年，请修改上面代码中if内容
        tradeDays=get_trade_days("2023-"+str(i)+"-01","2023-"+str(i)+"-"+str(calendar.monthrange(2023,i)[1]))
        df=query_iwencai(str(i)+"月 >3连板，非st")
        dfkeep=pd.DataFrame(columns=df.columns)
        #保存目标数据
        tradeRange=get_trade_days(startDate,endDate)
        numOfdfkeep=-1
        for num in range(len(df['股票代码'])):
            ticker=df.loc[num,'股票代码']
            days=df.loc[num,'连续涨停天数']
            value=get_price(ticker, str(tradeRange[0]), str(tradeRange[-1]),'1d',['open','close','low','high','high_limit','quote_rate','turnover_rate','prev_close','amp_rate'],True,None,0,False)
            for m in range(len(value)-days):
                mIndex=value.index[m]
                t=0
                k=0
                #k 是低换手出现次数
                #t 是连板次数
                if value.loc[mIndex,'close']==value.loc[mIndex,'high_limit']:
                    n=m
                    nIndex=value.index[n]
                    while value.loc[nIndex,'close']==value.loc[nIndex,'high_limit']:
                            highestBreak=value.loc[nIndex,'high']
                            t=t+1
                            n=n+1
                            nIndex=value.index[n]
                            if n==range(len(value))[-1]:break
                    #n 为断板当天的index数
                    if t>=days and (nIndex==tradeDays).sum()==0:
                        continue
                    #将非本月份的涨停去掉，因为value的时间维度是前后一共三个月，可能多次连板涨停，只取本月的
                    if t>=4 and (nIndex==tradeDays).sum()==1:
                        for j in range(2,t+1):
                            if value.loc[value.index[n-j],'turnover_rate']<=turnoverRate:
                                k=k+1
                        if k>=numOfLowTurnOver:
                            numOfdfkeep=numOfdfkeep+1
                            dfkeep.loc[numOfdfkeep]=df.loc[num]
                            dfkeep.loc[numOfdfkeep,'断板时间']=value.index[n]
        dataframe=dataframe.append(dfkeep,ignore_index=True)
    dataframe=dataframe.drop_duplicates()
    dataframe=dataframe.reset_index(drop=True)
    return dataframe 




import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import calendar

MonthRange=range(1,12,1)
tradeDays=get_all_trade_days()

def SiBanCeLueXuanGu(MonthRange):
    dataframe=pd.DataFrame()
    turnoverRate=8
    numOfLowTurnOver=2
    for i in MonthRange:
        if i==1:
            startDate="2022-12-01"
            endDate="2023-2-28"
        else:
            startDate="2023-"+str(i-1)+"-01"
            endDate="2023-"+str(i+1)+"-"+str(calendar.monthrange(2023,i+1)[1])
        #当所需要的数据范围不为2023年，请修改上面代码中if内容
        tradeDays=get_trade_days("2023-"+str(i)+"-01","2023-"+str(i)+"-"+str(calendar.monthrange(2023,i)[1]))
        df=query_iwencai(str(i)+"月 >3连板，非st")
        dfkeep=pd.DataFrame(columns=df.columns)
        #保存目标数据
        tradeRange=get_trade_days(startDate,endDate)
        numOfdfkeep=-1
        for num in range(len(df['股票代码'])):
            ticker=df.loc[num,'股票代码']
            days=df.loc[num,'连续涨停天数']
            value=get_price(ticker, str(tradeRange[0]), str(tradeRange[-1]),'1d',['open','close','low','high','high_limit','quote_rate','turnover_rate','prev_close','amp_rate'],True,None,0,False)
            for m in range(len(value)-days):
                mIndex=value.index[m]
                t=0
                k=0
                #k 是低换手出现次数
                #t 是连板次数
                if value.loc[mIndex,'close']==value.loc[mIndex,'high_limit']:
                    n=m
                    nIndex=value.index[n]
                    while value.loc[nIndex,'close']==value.loc[nIndex,'high_limit']:
                            highestBreak=value.loc[nIndex,'high']
                            t=t+1
                            n=n+1
                            nIndex=value.index[n]
                            if n==range(len(value))[-1]:break
                    #n 为断板当天的index数
                    if t>=days and (nIndex==tradeDays).sum()==0:
                        continue
                    #将非本月份的涨停去掉，因为value的时间维度是前后一共三个月，可能多次连板涨停，只取本月的
                    if t>=4 and (nIndex==tradeDays).sum()==1:
                        for j in range(1,3,1):
                            if value.loc[value.index[n-j],'turnover_rate']<=turnoverRate:
                                k=k+1
                        if k>=numOfLowTurnOver:
                            numOfdfkeep=numOfdfkeep+1
                            dfkeep.loc[numOfdfkeep]=df.loc[num]
                            dfkeep.loc[numOfdfkeep,'断板时间']=value.index[n]
        dataframe=dataframe.append(dfkeep,ignore_index=True)
    dataframe=dataframe.drop_duplicates()
    dataframe=dataframe.reset_index(drop=True)
    return dataframe