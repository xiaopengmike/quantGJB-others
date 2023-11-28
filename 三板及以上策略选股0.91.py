#三板及以上策略选股0.91
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import calendar

# DF = pd.DataFrame()

MonthRange=range(1,12,1)
tradeDays=get_all_trade_days()

def SanBanCeLueXuanGu(MonthRange):
    lowPriceToYesterday = 0.92
    ammRate = 5
    turnoverRate = 0.8

    print('深水捞最低位昨日收盘价比例='+str(lowPriceToYesterday))
    print('深水捞振幅='+str(ammRate))
    print('深水捞换手率相对断板日占比='+str(turnoverRate))

    dataframe=pd.DataFrame()
    for i in MonthRange:
        if i==1:
            startDate="2022-12-01"
            endDate="2023-2-28"
        else:
            startDate="2023-"+str(i-1)+"-01"
            endDate="2023-"+str(i+1)+"-"+str(calendar.monthrange(2023,i+1)[1])
        #当所需要的数据范围不为2023年，请修改上面代码中if内容
        tradeDays=get_trade_days("2023-"+str(i)+"-01","2023-"+str(i)+"-"+str(calendar.monthrange(2023,i)[1]))
        df=query_iwencai(str(i)+"月 >2连板，非st")
        tradeRange=get_trade_days(startDate,endDate)
        for num in range(len(df['股票代码'])):
            ticker=df.loc[num,'股票代码']
            # print(ticker)
            days=df.loc[num,'连续涨停天数']
            value=get_price(ticker, str(tradeRange[0]), str(tradeRange[-1]),'1d',['open','close','low','high','high_limit','quote_rate','turnover_rate','prev_close','amp_rate'],True,None,0,False)
            for m in range(len(value)-days):
                mIndex=value.index[m]
                #mIndex为循环的日期，m指代第几个日期，通过value.index[]函数来获取指定的日期
                t=0
                #t 是连板次数
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
                    #n 为断板当天的index数
                    #通过while循环来获取连板后的断板日期，但t连板次数不一定符合要求
                    if t>=days and (nIndex==tradeDays).sum()==0:
                        #条件是从所选日期到断板的连板次数至少为表格所示，且不在本月范围内，返回下一日期
                        continue
                    #将非本月份的涨停去掉，因为value的时间维度是前后一共三个月，可能多次连板涨停，只取本月的
                    if t>=days:
                        df.loc[num,'断板时间']=value.index[n]
                        dateList=[]
                        if n==range(len(value))[-1]:
                            if value.loc[nIndex,'amp_rate']>ammRate and value.loc[nIndex,'low']/value.loc[nIndex,'prev_close']<=lowPriceToYesterday and value.loc[nIndex,'close']>value.loc[nIndex,'open']:
                                dateList.append(n)
                                #记录第几个位置是符合条件的日期
                        elif n==range(len(value))[-2]:
                            for k in range(n,n+2,1):
                                if value.loc[value.index[k],'amp_rate']>ammRate and value.loc[value.index[k],'low']/value.loc[value.index[k],'prev_close']<=lowPriceToYesterday and value.loc[value.index[k],'close']>value.loc[value.index[k],'open']:
                                    if value.loc[value.index[k],'turnover_rate']/value.loc[nIndex,'turnover_rate']>=turnoverRate:
                                        dateList.append(k)
                                    #记录第几个位置是符合条件的日期
                        elif n==range(len(value))[-3]:
                        #断板后还有至少两天，算上断板当天，一共至少有三天
                            for k in range(n,n+3,1):
                                if value.loc[value.index[k],'amp_rate']>ammRate and value.loc[value.index[k],'low']/value.loc[value.index[k],'prev_close']<=lowPriceToYesterday and value.loc[value.index[k],'close']>value.loc[value.index[k],'open']:
                                    if value.loc[value.index[k],'turnover_rate']/value.loc[nIndex,'turnover_rate']>=turnoverRate:
                                        dateList.append(k)
                                    #记录第几个位置是符合条件的日期
                        elif n<=range(len(value))[-4]:
                        #断板后还有至少三天，算上断板当天，一共至少有四天
                            for k in range(n,n+4,1):
                                if value.loc[value.index[k],'amp_rate']>ammRate and value.loc[value.index[k],'low']/value.loc[value.index[k],'prev_close']<=lowPriceToYesterday and value.loc[value.index[k],'close']>value.loc[value.index[k],'open']:
                                    if value.loc[value.index[k],'turnover_rate']/value.loc[nIndex,'turnover_rate']>=turnoverRate:
                                        dateList.append(k)
                                    #记录第几个位置是符合条件的日期
                        if dateList!=[]:
                            Date=dateList[0]
                            df.loc[num,'符合条件的日期']=value.index[Date]
                            df.loc[num,'翘地板当天收盘价']=value.loc[value.index[Date],'close']
                            if Date==range(len(value))[-1]:continue
                            #确保index不会超出范围
                            qRate1=((value.loc[value.index[Date+1],'high']+value.loc[value.index[Date+1],'close'])/(2*value.loc[value.index[Date+1],'prev_close']))-1
                            df.loc[num,'后第一天平均最高涨跌幅']=qRate1
                            #qRate是指第几天的最高价和收盘价的平均相对翘地板当天的收盘价涨幅是多少
                            mRate1=value.loc[value.index[Date+1],'high']/value.loc[value.index[Date+1],'prev_close']-1
                            df.loc[num,'后第一天最高涨跌幅']=mRate1
                            #mRate是指第几天的最高价相对翘地板当天的收盘价涨幅是多少
                            df.loc[num,'后第一天开盘价']=value.loc[value.index[Date+1],'open']
                            df.loc[num,'后第一天最低价']=value.loc[value.index[Date+1],'low']
                            df.loc[num,'后第一天最高价']=value.loc[value.index[Date+1],'high']
                            df.loc[num,'后第一天收盘价']=value.loc[value.index[Date+1],'close']
                            
                            #记录符合条件的日期后的涨跌幅
                            if Date==range(len(value))[-2]:continue
                            
                            qRate2=((value.loc[value.index[Date+2],'high']+value.loc[value.index[Date+2],'close'])/(2*value.loc[value.index[Date+1],'prev_close']))-1
                            df.loc[num,'后第二天平均最高涨跌幅']=max(qRate1,qRate2)
                            #max(qRate1,qRate2)获取两天内平均的最高涨跌幅，不复利
                            mRate2=value.loc[value.index[Date+2],'high']/value.loc[value.index[Date+1],'prev_close']-1
                            df.loc[num,'后第二天最高涨跌幅']=max(mRate2,mRate1)
                            #max(mRate1,mRate2)获取两天内的最高涨跌幅，不复利
                            df.loc[num,'后第二天开盘价']=value.loc[value.index[Date+2],'open']
                            df.loc[num,'后第二天最低价']=value.loc[value.index[Date+2],'low']
                            df.loc[num,'后第二天最高价']=value.loc[value.index[Date+2],'high']
                            df.loc[num,'后第二天收盘价']=value.loc[value.index[Date+2],'close']
                            
                            if Date==range(len(value))[-3]:continue
                            
                            qRate3=((value.loc[value.index[Date+3],'high']+value.loc[value.index[Date+3],'close'])/(2*value.loc[value.index[Date+1],'prev_close']))-1
                            df.loc[num,'后第三天平均最高涨跌幅']=max(qRate1,qRate2,qRate3)
                            mRate3=value.loc[value.index[Date+3],'high']/value.loc[value.index[Date+1],'prev_close']-1
                            df.loc[num,'后第三天最高涨跌幅']=max(mRate1,mRate2,mRate3)
                            df.loc[num,'后第三天开盘价']=value.loc[value.index[Date+3],'open']
                            df.loc[num,'后第三天最低价']=value.loc[value.index[Date+3],'low']
                            df.loc[num,'后第三天最高价']=value.loc[value.index[Date+3],'high']
                            df.loc[num,'后第三天收盘价']=value.loc[value.index[Date+3],'close']
                            
                            
                            if max(qRate1,qRate2)==qRate1 and max(qRate1,qRate2)!=qRate2:
                                df.loc[num,'三天内最高平均盈利出现在第几天']="第一天"
                            elif max(qRate1,qRate2,qRate3)==qRate2 and max(qRate1,qRate2,qRate3)!=qRate3:
                                df.loc[num,'三天内最高平均盈利出现在第几天']="第二天"
                            else:
                                df.loc[num,'三天内最高平均盈利出现在第几天']="第三天"
                            if max(mRate1,mRate2)==mRate1 and max(mRate1,mRate2)!=mRate2:
                                df.loc[num,'三天内最高盈利出现在第几天']="第一天"
                            elif max(mRate1,mRate2,mRate3)==mRate2 and max(mRate1,mRate2,mRate3)!=mRate3:
                                df.loc[num,'三天内最高盈利出现在第几天']="第二天"
                            else:
                                df.loc[num,'三天内最高盈利出现在第几天']="第三天"
                                
        dataframe = dataframe.append(df, ignore_index=True)
    dataframe = dataframe.dropna(subset=['符合条件的日期'])
    dataframe = dataframe.reset_index(drop=True)
    #重置索引，便于增加一行胜率
    l=len(dataframe)
    dataframe.loc[l]=np.nan
    # dataframe.loc[l+1,'符合条件的日期']='胜率'
    # dataframe.loc[l,'符合条件的日期']='mean'
    
    winRateMean1=len(dataframe[dataframe.后第一天平均最高涨跌幅>0])/len(dataframe.后第一天平均最高涨跌幅.dropna())
    #后第一天平均最高涨跌幅的胜率
    winRateMean2=len(dataframe[dataframe.后第二天平均最高涨跌幅>0])/len(dataframe.后第二天平均最高涨跌幅.dropna())
    #后第二天平均最高涨跌幅的胜率
    winRateMean3=len(dataframe[dataframe.后第三天平均最高涨跌幅>0])/len(dataframe.后第三天平均最高涨跌幅.dropna())
    #后第三天平均最高涨跌幅的胜率
    winRate1=len(dataframe[dataframe.后第一天最高涨跌幅>0])/len(dataframe.后第一天最高涨跌幅.dropna())
    #后第一天最高涨跌幅的胜率
    winRate2=len(dataframe[dataframe.后第二天最高涨跌幅>0])/len(dataframe.后第二天最高涨跌幅.dropna())
    #后第二天最高涨跌幅的胜率
    winRate3=len(dataframe[dataframe.后第三天最高涨跌幅>0])/len(dataframe.后第三天最高涨跌幅.dropna())
    #后第三天最高涨跌幅的胜率
    
    meanProfitAve1 = dataframe['后第一天平均最高涨跌幅'].mean()
    meanProfitAve2 = dataframe['后第二天平均最高涨跌幅'].mean()
    meanProfitAve3 = dataframe['后第三天平均最高涨跌幅'].mean()
    meanProfitHigh1 = dataframe['后第一天最高涨跌幅'].mean()
    meanProfitHigh2 = dataframe['后第二天最高涨跌幅'].mean()
    meanProfitHigh3 = dataframe['后第三天最高涨跌幅'].mean()
    
    dataframe.loc[l,'符合条件的日期']='mean'
    dataframe.loc[l+1,'符合条件的日期']='胜率'
    
    def toPercent(num):
        return str(round(num*100,2)) + '%'

    dataframe.loc[l,'后第一天平均最高涨跌幅']=dataframe['后第一天平均最高涨跌幅'].mean()
    dataframe.loc[l,'后第一天最高涨跌幅']=dataframe['后第一天最高涨跌幅'].mean()
    dataframe.loc[l+1,'后第一天平均最高涨跌幅']=winRateMean1
    dataframe.loc[l+1,'后第一天最高涨跌幅']=winRate1
    print('出现的次数'+str(len(dataframe)))
    print('---')
    print('winRate-后第一天平均最高涨跌幅='+toPercent(winRateMean1))
    print('Mean'+'后第一天平均最高涨跌幅='+toPercent(meanProfitAve1))
    print('winRate-后第一天最高涨跌幅='+toPercent(winRate1))
    print('Mean'+'后第一天最高涨跌幅='+toPercent(meanProfitHigh1))
    
    dataframe.loc[l+1,'后第二天平均最高涨跌幅']=winRateMean2
    dataframe.loc[l+1,'后第二天最高涨跌幅']=winRate1
    dataframe.loc[l,'后第二天平均最高涨跌幅']=dataframe['后第二天平均最高涨跌幅'].mean()
    dataframe.loc[l,'后第二天最高涨跌幅']=dataframe['后第二天最高涨跌幅'].mean()
    print('---')
    print('winRate-后第二天平均最高涨跌幅='+toPercent(winRateMean2))
    print('Mean'+'后第二天平均最高涨跌幅='+toPercent(meanProfitAve2))
    print('winRate-后第二天最高涨跌幅='+toPercent(winRate2))
    print('Mean'+'后第二天最高涨跌幅='+toPercent(meanProfitHigh2))

    dataframe.loc[l,'后第三天平均最高涨跌幅']=dataframe['后第三天平均最高涨跌幅'].mean()
    dataframe.loc[l,'后第三天最高涨跌幅']=dataframe['后第三天最高涨跌幅'].mean()

    dataframe.loc[l+1,'后第三天平均最高涨跌幅']=winRateMean3
    dataframe.loc[l+1,'后第三天最高涨跌幅']=winRate3
    print('---')
    print('winRate-后第二天平均最高涨跌幅='+toPercent(winRateMean3))
    print('Mean'+'后第二天平均最高涨跌幅='+toPercent(meanProfitAve3))
    print('winRate-后第二天最高涨跌幅='+toPercent(winRate3))
    print('Mean'+'后第二天最高涨跌幅='+toPercent(meanProfitHigh3))
    
    #Df=dataframe
    
    return dataframe

SanBanCeLueXuanGu(MonthRange)