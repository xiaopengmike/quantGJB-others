# -*- 导出指数相关数据 -*-
import pandas as pd
import datetime
from datetime import datetime
import asyncio
import aiohttp
import requests
import time
from openpyxl import Workbook

#先串行1900只指数股票，包括中证50，沪深300，中证500，中证1000
#后并发3100只股票，包括中证2000和其他非指数股票
#先创建两个dataframe，第一个包含所有1900只串行股票，第二个包含3100只并发股票



#沪深300 000300.SH or 399300.SZ
#中证500 000905.SH or 399905.SZ
#中证1000 000852.SH or 399852.SZ
#中证2000 932000.CSI
#中证科创创业50 931643.CSI

#code in SuperMind

#import pandas
#import numpy
#data=get_all_securities('index')
#hs300=get_index_stocks('000300.SH')
#zz500=get_index_stocks('000905.SH')
#zz1000=get_index_stocks('000852.SH')
#zz2000=get_index_stocks('932000.CSI')
#kccy50=get_index_stocks('931643.CSI')
#indexAll=kccy50+hs300+zz500+zz1000+zz2000
#df=pd.DataFrame([kccy50,hs300,zz500,zz1000,zz2000],index=['kccy50','hs300','zz500','zz1000','zz2000']).transpose()
#Df=pd.DataFrame(indexAll,columns=['symbol'])
#df.to_excel('IndexStocks.xlsx',sheet_name='index')
#Df.to_excel('allIndexStocks.xlsx',sheet_name='allIndexStocks',index=False)

wb = pd.ExcelWriter('./东财看涨看跌数据_指数分类.xlsx', engine='openpyxl')

indexFile=pd.read_excel('IndexStocks.xlsx')
#indexFile包含了四列指数的数据
allIndexFile=pd.read_excel('allIndexStocks.xlsx')
#allindexFile是一列数据，包含四个指数的数据
data=pd.read_excel('stocks.xlsx',sheet_name=0)
#data是所有A股股票的数据
tickerList=data[['symbol','display_name']]
allIndex=pd.merge(allIndexFile,tickerList,on='symbol')
#该合并操作是为了在allindex的数据中加入股票名称，方便后续函数运行
allIndex=allIndex.drop_duplicates()
#去重
allIndex = allIndex.reset_index(drop=True)
#重置索引，支持后续以索引为媒介的遍历’for‘结构
allIndex=allIndex[allIndex.index<850]
indexList=[]
for i in allIndex['symbol']:
    iNumber=(i==tickerList['symbol']).argmax()
    indexList.append(iNumber)
tickerListLeft=tickerList.drop(indexList)
tickerListLeft = tickerListLeft.reset_index(drop=True)

df=pd.DataFrame()
dfhs300=pd.DataFrame()
dfkccy50=pd.DataFrame()
dfzz500=pd.DataFrame()
dfzz1000=pd.DataFrame()
dfzz2000=pd.DataFrame()
#生成所有数据结果，全局变量方便函数内修改DataFrame数据
count=0
async def getFromDongFangCaiFu(tickername,tickerNumber,tickerLocation):
    dataf=pd.DataFrame()
    global df,dfhs300,dfkccy50,dfzz500,dfzz1000,dfzz2000, count
    async with aiohttp.ClientSession() as session:
        async with session.get('https://eminterservice.eastmoney.com/UserData/GetWebTape?code=' + tickerLocation + tickerNumber) as resp:
            print(resp.status)
            text = await resp.text()
            # print(res[:100])

            index = text.find('TapeZ')
            if index == -1:
                dataf.loc[0,tickername] = None
            else:
                count=count+1
                print(count)
                percent = text[41:48]
                index1 = percent.find(',')
                dataf.loc[0,tickername] = float(percent[:index1])
            df=pd.concat([df, dataf], axis=1)
            #查找股票所归属的指数
            if ((tickerNumber + '.' + tickerLocation) == indexFile['zz500']).sum().sum() == 1:
                dfzz500 = pd.concat([dfzz500, dataf], axis=1)
            if ((tickerNumber + '.' + tickerLocation) == indexFile['zz1000']).sum().sum() == 1:
                dfzz1000 = pd.concat([dfzz1000, dataf], axis=1)
            if ((tickerNumber + '.' + tickerLocation) == indexFile['zz2000']).sum().sum() == 1:
                dfzz2000 = pd.concat([dfzz2000, dataf], axis=1)

    return df


def CgetFromDongFangCaiFu(tickername,tickerNumber,tickerLocation):
    dataf=pd.DataFrame()
    global df,dfhs300,dfkccy50,dfzz500,dfzz1000,dfzz2000
    resp=requests.get('https://eminterservice.eastmoney.com/UserData/GetWebTape?code=' + tickerLocation + tickerNumber)
    print(resp.status_code)
    text = resp.text
    # print(res[:100])
    index = text.find('TapeZ')
    if index == -1:
        dataf.loc[0,tickername] = None
    else:
        percent = text[41:48]
        index1 = percent.find(',')
        dataf.loc[0,tickername] = float(percent[:index1])
    df=pd.concat([df, dataf], axis=1)
    #查找股票所归属的指数
    if ((tickerNumber + '.' + tickerLocation) == indexFile['hs300']).sum().sum() == 1:
         dfhs300 = pd.concat([dfhs300, dataf], axis=1)
    if ((tickerNumber + '.' + tickerLocation) == indexFile['kccy50']).sum().sum() == 1:
         dfkccy50 = pd.concat([dfkccy50, dataf], axis=1)
    if ((tickerNumber + '.' + tickerLocation) == indexFile['zz500']).sum().sum() == 1:
         dfzz500 = pd.concat([dfzz500, dataf], axis=1)
    if ((tickerNumber + '.' + tickerLocation) == indexFile['zz1000']).sum().sum() == 1:
         dfzz1000 = pd.concat([dfzz1000, dataf], axis=1)
    if ((tickerNumber + '.' + tickerLocation) == indexFile['zz2000']).sum().sum() == 1:
         dfzz2000 = pd.concat([dfzz2000, dataf], axis=1)
    return df
#串行函数


async def main():
    global df
    Time=(datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
    df.loc[0,'日期时间']=Time
    dfkccy50.loc[0, '日期时间'] = Time
    dfzz500.loc[0, '日期时间'] = Time
    dfzz1000.loc[0, '日期时间'] = Time
    dfzz2000.loc[0, '日期时间'] = Time
    dfhs300.loc[0, '日期时间'] = Time
    #pd.set_option('display.max_colwidth',10)
    global count
    t50=0
    t300=0
    t500=0
    t1000=0
    t2000=0
    startTimeForChuanXing=time.time()
    for i in range(len(allIndex)):
        t=allIndex.loc[i, 'symbol']
        name=allIndex.loc[i, 'display_name']
        tickerNumber=t[:6]
        tickerLocation=t[-2:]
        ticker=tickerNumber+tickerLocation
        tickername=name+ticker
        print(tickername)
        # x=requests.get('https://eminterservice.eastmoney.com/UserData/GetWebTape?code='+tickerLocation+tickerNumber)
        # text=x.text
        CgetFromDongFangCaiFu(tickername,tickerNumber,tickerLocation)
        count=count+1
        print(count)
        if dfkccy50.shape[1]==51 and t50==0:
            dfkccy50.to_excel(wb, sheet_name='中证50_index_data', index=False)
            wb.save()
            t50=t50+1
        if dfhs300.shape[1]==301 and t300==0:
            dfhs300.to_excel(wb, sheet_name='沪深300_index_data', index=False)
            wb.save()
            t300=t300+1
        if dfzz500.shape[1]==501 and t500==0:
            dfzz500.to_excel(wb, sheet_name='中证500_index_data', index=False)
            wb.save()
            t500=t500+1
        if dfzz1000.shape[1]==1001 and t1000==0:
            dfzz1000.to_excel(wb, sheet_name='中证1000_index_data', index=False)
            wb.save()
            t1000=t1000+1
    print("串行时间： ")
    print(time.time()-startTimeForChuanXing)
    startTimeForBingXing=time.time()
    taskList1=[]
    taskList2=[]
    taskList3=[]
    for j in range(len(tickerListLeft)):
        t=tickerListLeft.loc[j, 'symbol']
        name=tickerListLeft.loc[j, 'display_name']
        tickerNumber=t[:6]
        tickerLocation=t[-2:]
        ticker=tickerNumber+tickerLocation
        tickername=name+ticker
        print(tickername)
        # x=requests.get('https://eminterservice.eastmoney.com/UserData/GetWebTape?code='+tickerLocation+tickerNumber)
        # text=x.text
        if j<=(len(tickerListLeft)/3):
            task1=asyncio.create_task(getFromDongFangCaiFu(tickername,tickerNumber,tickerLocation))
            taskList1.append(task1)
        elif j<=(len(tickerListLeft)*2/3):
            task2 = asyncio.create_task(getFromDongFangCaiFu(tickername, tickerNumber, tickerLocation))
            taskList2.append(task2)
        else:
            task3 = asyncio.create_task(getFromDongFangCaiFu(tickername, tickerNumber, tickerLocation))
            taskList3.append(task3)
    done1, pending1 = await asyncio.wait(taskList1, timeout=0)
    await asyncio.sleep(1)
    done2, pending2 = await asyncio.wait(taskList2, timeout=0)
    await asyncio.sleep(1)
    done3, pending3 = await asyncio.wait(taskList3, timeout=0)
    print("并行时间： ")
    print(time.time() - startTimeForBingXing)
    df.to_excel(wb,sheet_name='all_index_data',index=False)
    if t1000==0:
        dfzz1000.to_excel(wb, sheet_name='中证1000_index_data', index=False)
    if t2000==0:
        dfzz2000.to_excel(wb, sheet_name='中证2000_index_data', index=False)
    wb.save()
    return df
if __name__ == '__main__':
    startTime=time.time()
    event_loop = asyncio.get_event_loop()
    result = event_loop.run_until_complete(asyncio.gather(main()))
    event_loop.close()
    print(result)
    print(time.time()-startTime)