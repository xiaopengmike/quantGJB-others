# -*- 导出指数相关数据 -*-
import pandas as pd
import datetime
from datetime import datetime
import asyncio
import aiohttp
import requests
import time
from openpyxl import Workbook

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
allindexFile=pd.read_excel('allIndexStocks.xlsx')
#allindexFile是一列数据，包含四个指数的数据
data=pd.read_excel('stocks.xlsx',sheet_name=0)
#data是所有A股股票的数据
tickerList=data[['symbol','display_name']]
allindex=pd.merge(allindexFile,tickerList,on='symbol')
#该合并操作是为了在allindex的数据中加入股票名称，方便后续函数运行
allindex=allindex.drop_duplicates()
#去重
allindex = allindex.reset_index(drop=True)
#重置索引，支持后续以索引为媒介的遍历’for‘结构
indexList=[]
for i in allindex['symbol']:
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

async def getFromDongFangCaiFu(tickername,tickerNumber,tickerLocation):
    dataf=pd.DataFrame()
    global df,dfhs300,dfkccy50,dfzz500,dfzz1000,dfzz2000
    async with aiohttp.ClientSession() as session:
        async with session.get('https://eminterservice.eastmoney.com/UserData/GetWebTape?code=' + tickerLocation + tickerNumber) as resp:
            print(resp.status)
            text = await resp.text()
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

async def getFromDongFangCaiFuLeft(tickername,tickerNumber,tickerLocation):
    dataf=pd.DataFrame()
    global df
    async with aiohttp.ClientSession() as session:
        async with session.get('https://eminterservice.eastmoney.com/UserData/GetWebTape?code=' + tickerLocation + tickerNumber) as resp:
            print(resp.status)
            text = await resp.text()
            # print(res[:100])
            index = text.find('TapeZ')
            if index == -1:
                dataf.loc[0,tickername] = None
            else:
                percent = text[41:48]
                index1 = percent.find(',')
                dataf.loc[0,tickername] = float(percent[:index1])
            df=pd.concat([df, dataf], axis=1)
    return df

async def main():
    global df
    Time=(datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
    df.loc[0,'日期时间']=Time
    pd.set_option('display.max_colwidth',10)
    taskList=[]
    for i in range(len(allindex)):
        t=allindex.loc[i, 'symbol']
        name=allindex.loc[i, 'display_name']
        tickerNumber=t[:6]
        tickerLocation=t[-2:]
        ticker=tickerNumber+tickerLocation
        tickername=name+ticker
        print(tickername)
        # x=requests.get('https://eminterservice.eastmoney.com/UserData/GetWebTape?code='+tickerLocation+tickerNumber)
        # text=x.text
        task=asyncio.create_task(getFromDongFangCaiFu(tickername,tickerNumber,tickerLocation))
        taskList.append(task)
    done, pending = await asyncio.wait(taskList, timeout=70)
    taskList2=[]
    for i in range(len(tickerListLeft)):
        t=allindex.loc[i, 'symbol']
        name=allindex.loc[i, 'display_name']
        tickerNumber=t[:6]
        tickerLocation=t[-2:]
        ticker=tickerNumber+tickerLocation
        tickername=name+ticker
        print(tickername+'非指数')
        # x=requests.get('https://eminterservice.eastmoney.com/UserData/GetWebTape?code='+tickerLocation+tickerNumber)
        # text=x.text
        task2=asyncio.create_task(getFromDongFangCaiFu(tickername,tickerNumber,tickerLocation))
        taskList2.append(task2)
    done2, pending2 = await asyncio.wait(taskList2, timeout=70)

    df.to_excel(wb,sheet_name='all_index_data',index=False)
    dfkccy50.to_excel(wb, sheet_name='中证50_index_data', index=False)
    dfhs300.to_excel(wb, sheet_name='沪深300_index_data', index=False)
    dfzz500.to_excel(wb, sheet_name='中证500_index_data', index=False)
    dfzz1000.to_excel(wb, sheet_name='中证1000_index_data', index=False)
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