import pandas as pd
import requests
import sys


def getHeatMonitoringRankOfStock(stockID): #stock 是包含交易所的股票代码，如000001.SH
    DF=pd.DataFrame() #DF为最后返回结果
    rowOfDF=10 #最后返回前十排名最高的数据结果
    #stockID股票代码中的数字，用于生成目标URL
    if stockID[0]=='6':
        marketCode='17'
    elif stockID[0]=='0' or stockID[0]=='3':
        marketCode='33'
    else:
        print('Error: Input is wrong. Can\'t get the correct market code.')
        return DF
    targetURL="https://ms.10jqka.com.cn/index/robotindex/?appName=iwc_rbxq&codes="+stockID+"&code="+stockID+"&marketCode="+marketCode+"&market_code="+marketCode+"&market="+marketCode+"&uuid=30235&stock_market="+stockID+"%2C"+marketCode+"&user_id=0"
    x=requests.get(targetURL) #获取数据
    xDict=x.json() #将从服务器获取的数据转换为字典，便于寻找数据
    answerDict=xDict.get('answer') #获取字典中的‘answer’字典
    compList=answerDict.get('components') #获取answer字典的’components‘ 列表
    targetDict=compList[1].get('data')[4] #获取包含每日热度的字典
    targetList=targetDict.get('datas') #获取字典中的数据，列表的每一个数据都是一个字典，以{'time': '11-05', '排名': 2775, 'timeNew': '2023-11-05 23:00:00'}形式存在
    for i in range(len(targetList)):
        df=pd.DataFrame() #储存每一条热度数据的dataframe
        dateStr=targetList[i].get('timeNew') #包含小时分钟秒
        date=dateStr.split()[0] #获取年月日数据
        df.loc[0,'日期']=date
        df.loc[0,'热度排名']=targetList[i].get('排名')
        DF=pd.concat([DF,df],axis=0, ignore_index=True)
    DF=DF.sort_values(by='热度排名',ascending=True)[:rowOfDF] #按排名数字升序排列并取前n行
    DF=DF.reset_index(drop=True) #重置索引
    DF['热度排名']=DF['热度排名'].apply(int) #将热度排名由float变为int
    print(DF)
    return DF

if __name__=="__main__":
    if len(sys.argv)<2:
        print('Fail to get the stockID.')
    if len(sys.argv)>=2:
        for i in range(1,len(sys.argv),1):
            print("for stockID="+sys.argv[i]+", the top 10 HeatMonitoring Ranks in the last month are the following:")
            getHeatMonitoringRankOfStock(sys.argv[i])
        
        


