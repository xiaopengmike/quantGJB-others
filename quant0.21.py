# -*- encoding: utf-8 -*-
import pandas as pd
import datetime
from datetime import datetime
import asyncio
import aiohttp
import requests
import time

data=pd.read_excel('stocks.xlsx',sheet_name=0)
#每个代理IP最多可以访问目标网站100次
#自己的IP最多可以并发访问网站4000次，但后1000次会卡顿
#使用golbal dataframe来优化代码

def getProxyUrl():
        proxyAddrUrl="https://share.proxy.qg.net/get?key=5RFZ7QKU&num=1&area=&isp=&format=json&seq=&distinct=false&pool=1"
        resp = requests.get(proxyAddrUrl)
        x = resp.text.split("server")
        return x
def oneProxyUrl(i,proxyUrlList):
    if i >len(proxyUrlList):
        print("i is not in length of proxyUrlList")
        return
    else:
        x= proxyUrlList[i]
        index = x.find(',')
        proxyAddr = x[3:index-1]
        authKey = "5RFZ7QKU"
        password = "EAA59EA0C819"
        proxyUrl = "http://%(user)s:%(password)s@%(server)s" % {
            "user": authKey,
            "password": password,
            "server": proxyAddr,
        }
        return proxyUrl

df=pd.DataFrame()

async def getFromDongFangCaiFu(tickername,tickerNumber,tickerLocation):
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
                df=pd.concat([df,dataf],axis=1)
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
    for i in range(2500):
        if i>=len(data): break
        t=data.loc[i, 'symbol']
        name=data.loc[i, 'display_name']
        tickerNumber=t[:6]
        tickerLocation=t[-2:]
        ticker=tickerNumber+tickerLocation
        tickername=name+ticker
        print(tickername)
        # x=requests.get('https://eminterservice.eastmoney.com/UserData/GetWebTape?code='+tickerLocation+tickerNumber)
        # text=x.text
        task=asyncio.create_task(getFromDongFangCaiFu(tickername,tickerNumber,tickerLocation))
        taskList.append(task)
    done, pending = await asyncio.wait(taskList, timeout=50)
    return df
if __name__ == '__main__':
    startTime=time.time()
    event_loop = asyncio.get_event_loop()
    result = event_loop.run_until_complete(asyncio.gather(main()))
    event_loop.close()
    print(result)
    print(time.time()-startTime)