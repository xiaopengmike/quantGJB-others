# 同花顺热股排行榜5分钟热度获取_近一天

import pandas as pd
import requests
from sqlalchemy import create_engine


def getAllStocks():
    url = "https://dq.10jqka.com.cn/fuyao/hot_list_data/out/hot_list/v1/stock?stock_type=a&type=day&list_type=normal"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    }
    rep = requests.get(url, headers=headers)
    if rep.status_code != 200:
        print(f'Request failed with status code: {rep.status_code}')
    else:
        repCtent = rep.json()
        stockList = repCtent.get('data').get('stock_list')
        return stockList
#expected output: 一个包含100个热股的List，其中每一项的格式为以下格式，个别包含'analyse'和'analyse_title'两个键，内容为针对热股的分析
# { "market": 33,
#   "code": "000977",
#   "rate": "105272.0",
#   "rise_and_fall": -2.2752,
#   "name": "浪潮信息",
#   "hot_rank_chg": 0,
#   "tag": {
#     "concept_tag": [
#      "英伟达概念",
#      "国产操作系统"
#     ]
#   },
#   "order": 78
# }

def getHeatRankDayByhours(name, stockID, market):
    DF = pd.DataFrame()
    marketCode = str(market)
    # 将输入的"int" market变量变为string
    url = "https://ms.10jqka.com.cn/index/robotindex/?appName=iwc_rbxq&codes="+stockID+"&code="+stockID+"&marketCode="+marketCode+"&market_code="+marketCode+"&market="+marketCode+"&uuid=30235&stock_market="+stockID+"%2C"+marketCode+"&user_id=651927008"
    rep = requests.get(url)
    repCtent = rep.json()
    dataList = repCtent.get('answer').get('components')[1].get('data')[3].get('datas')
    # dataList是一个包含了一天内每5分钟的热度排名的list，格式为 {date: "2023-12-13 00:00", time: "00:00", 排名: 6}
    for i in range(len(dataList)):
        dateTime = dataList[i].get('date')
        date = dateTime[:10]
        time = dataList[i].get('time')
        rank = dataList[i].get('排名')
        df = pd.DataFrame([[stockID, name, date, time, rank]])
        DF = pd.concat([DF, df], axis=0, ignore_index=True)
    DF.columns = ['股票代码', '股票简称', '日期', '时间', '热度排名']
    return DF

if __name__ == "__main__":
    stockList = getAllStocks()
    df = pd.DataFrame()
    engine = create_engine('mysql+pymysql://root:Ggao123456@rm-wz9tp0e81s06obulcyo.mysql.rds.aliyuncs.com:3306/heatrank_db')

    for n in range(len(stockList)):
        stock = stockList[n]
        stockName = stock.get('name')
        stockID = stock.get('code')
        market = stock.get('market')
        print(stockName)
        print(stockID)
        DF = getHeatRankDayByhours(stockName, stockID, market)
        df = pd.concat([df, DF], axis=0, ignore_index=True)
    df.to_sql('同花顺热股排行榜热度_近一天', engine, if_exists='append', index=False)
