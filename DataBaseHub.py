import pymysql
from pymongo import MongoClient
import pandas as pd
from pprint import pprint
import Utility
import logging
import jqdatasdk as jq
from sklearn import preprocessing
import seaborn as sns
import matplotlib.pyplot as plt

dblogger = logging.getLogger(__name__)


class JQData:
    def __init__(self):
        if not jq.is_auth():
            jq.auth('15982102544', 'kevinxli_035')

    #Stock
    def getallstocks(self):
        return list(jq.get_all_securities(['stock']).index)

    def normalizecode(self, codes):
        return jq.normalize_code(codes)

    def getstockname(self, code):
        return jq.get_security_info(code).display_name

    def getStockDailyCandle(self, s, tday, num):
        df = jq.get_price(s, end_date=tday, count=num, frequency='daily', fields=None)
        df['code'] = s
        return df

    def getindustry(self, code, tday):
        return jq.get_industry(code, tday)



    #Futures
    def get_future_contracts(self, p, tday):
        return jq.get_future_contracts(p, tday)

    def get_trade_days(self, s, e):
        return jq.get_trade_days(start_date=s, end_date=e)

    def getContractDailyCandle(self, c, p, tday, num):
        df = jq.get_price(c, end_date=tday, count=num, frequency='daily', fields=None)
        df['Contract'] = c
        df['Date'] = tday
        df['product'] = p
        return df

    def getDominantFuture(self, p, tday):
        return jq.get_dominant_future(p, tday)

    def getedate(self, c):
        return jq.get_security_info(c).end_date


class DataBaseHub:
    def __init__(self):
        self.sqlalpha = pymysql.connect('119.3.130.46', 'root', 'gene@2019', 'longshort', port=3306)
        self.alcursor = self.sqlalpha.cursor()

        self.sqlcc = pymysql.connect('119.3.130.46', 'root', 'gene@2019', 'ccalpha', port=3306)
        self.cccursor = self.sqlcc.cursor()

        self.sqlcta = pymysql.connect('119.3.130.46', 'root', 'gene@2019', 'ctaalpha', port=3306)
        self.ctacursor = self.sqlcta.cursor()

        self.sqllocmk = pymysql.connect('192.168.18.7', 'root', '123$%^qweRTY', 'MarketData', port=3306)
        self.locmkcursor = self.sqllocmk.cursor()

        self.sqllocss = pymysql.connect('192.168.18.7', 'root', '123$%^qweRTY', 'StrategyStatus', port=3306)
        self.locsscursor = self.sqllocss.cursor()

        self.sqllocpos = pymysql.connect('192.168.18.7', 'root', '123$%^qweRTY', 'PortfolioPosition', port=3306)
        self.locppcursor = self.sqllocpos.cursor()

        self.mgclient = MongoClient('192.168.18.7', 27017)
        self.futureaccount = self.mgclient.FutureAccount

    def __fetchdata(self, sql, cur, cls):
        try:
            cur.execute(sql)
            res = cur.fetchall()
            if len(res) > 0:
                df = pd.DataFrame(list(res))
                df.columns = cls
                return df
            else:
                return None
        except:
            print('Error:{}'.format(sql))
            return None

    def __savedata(self, sql, dbs, cur):
        try:
            cur.execute(sql)
            dbs.commit()
        except:
            print('Error:{}'.format(sql))

    def QuerySingleStockScoreDaily(self, num, code, tday):
        table = "alpha_"+str(num).zfill(3)
        sql = "select * from {} where code = '{}' and TradeDate = '{}'".format(table, code, tday)
        cls = ['code', 'Name', 'TradeDate', 'factor', 'score']
        return self.__fetchdata(sql, self.alcursor, cls)

    def QueryMultiStocksScoreDaily(self, nums, code, tday):
        dfs = []
        for num in nums:
            dfs.append(self.QuerySingleStockScoreDaily(num, code, tday))
        return pd.concat(dfs)

    def QuerySinglealphaDaily(self, num, tday):
        table = "alpha_"+str(num).zfill(3)
        sql = "select * from {} where TradeDate = {}".format(table, tday)
        cls = ['Code', 'Name', 'TradeDate', 'factor', 'score']
        df = self.__fetchdata(sql, self.alcursor, cls)
        try:
            df['discrete_score'] = pd.qcut(df['score'], q=5, labels=False, precision=10) - 2
            return df
        except:
            return None

    def QueryMultialphaDaily(self, nums, tday):
        dfs = []
        for num in nums:
            dfs.append(self.QuerySinglealphaDaily(num, tday))
            print('load alpha {}'.format(num))
        return pd.concat(dfs)

    def QueryStockPool(self):
        sql = "select * from stockpool"
        cls = ['Code', 'Name', 'sw_l2', 'jq_l2', 'EntryDate']
        return self.__fetchdata(sql, self.cccursor, cls)

    def QueryStockRanking(self, table, tday):
        sql = "select * from {} where TradeDate = {}".format(table, tday)
        cls = ['Code', 'Name', 'TradeDate', 'score']
        return self.__fetchdata(sql, self.cccursor, cls)

    def SaveScore(self, df, table):
        #df.to_sql(con=self.sqlcc, name='ewscore', if_exists='replace', index=False)
        for index, row in df.iterrows():
            sql = "insert into {} VALUE ('{}', '{}', '{}',{:4f});".format(table, row['Code'], row['Name'], row['TradeDate'], row['score'])
            self.__savedata(sql, self.sqlcc, self.cccursor)

    def SaveStockPool(self, code, name, tday, swi, jqi):
        sql = "insert into stockpool VALUE ('{}', '{}', '{}', '{}', '{}') on duplicate key update " \
              "sw_l2 = '{}', jq_l2 = '{}', EntryDate = '{}';".format(code, name, swi, jqi, tday, swi, jqi, tday)
        self.__savedata(sql, self.sqlcc, self.cccursor)

    # Futures
    def SaveContractDailyCandle(self, df):
        sql = "insert into dailycandle VALUE ('{}', '{}', {},{}, {}, {}, {}, {},'{}')" \
            .format(df['Contract'].values[0], df['product'].values[0], df['open'].values[0],
                    df['high'].values[0], df['low'].values[0],  df['close'].values[0],
                    df['volume'].values[0], df['money'].values[0], df['Date'].values[0])
        self.__savedata(sql, self.sqlcta, self.ctacursor)

    def QueryDailyProductPrice(self, p, tday):
        sql = "select * from dailycandle where product = '{}' and Date = '{}'".format(p, tday)
        cls = ['contract', 'product', 'open', 'high', 'low', 'close', 'volume', 'money', 'date']
        return self.__fetchdata(sql, self.ctacursor, cls)

    def QueryDailyCandles(self, sdate, edate):
        sql = "select * from dailycandle where Date >= '{}' and Date <= '{}'".format(sdate, edate)
        cls = ['contract', 'product', 'open', 'high', 'low', 'close', 'volume', 'money', 'date']
        return self.__fetchdata(sql, self.ctacursor, cls)

    def SaveFactorInfo(self, p, dom, tday):
        sql = "insert into factors (product, Date, dominate) VALUE ('{}', '{}', '{}')" .format(p, tday, dom)
        self.__savedata(sql, self.sqlcta, self.ctacursor)

    def QueryFactorInfo(self, factor, sdate, edate):
        sql = "select product,Date,dominate,{} from factors where Date >='{}' and Date <= '{}'".format(factor, sdate, edate)
        cls = ['product', 'date', 'dominate', factor]
        return self.__fetchdata(sql, self.ctacursor, cls)

    def Saverollyield(self, p, tday, ry_abs, ry_yy):
        sql = "update factors set ry_abs = {:4f}, ry_yy = {:4f} where product = '{}' and Date = '{}';".format(ry_abs, ry_yy, p, tday)
        self.__savedata(sql, self.sqlcta, self.ctacursor)

    def SaveFutureBasisDaily(self, df):
        sql = "insert into futurebasis VALUE ('{}',{:4f},'{}',{},'{}', {},'{}')" \
            .format(df['product'].values[0], (df['close'].values[0] - df['close'].values[1]), df['contract'].values[0],
                    df['close'].values[0], df['contract'].values[1], df['close'].values[1], df['date'].values[0])
        self.__savedata(sql, self.sqlcta, self.ctacursor)

    def QueryFutureBasisDaily(self, tday):
        sql = "select * from futurebasis where Date = '{}'".format(tday)
        cls = ['product', 'basis', 'near', 'nprice', 'far', 'fprice', 'date']
        return self.__fetchdata(sql, self.sqlcta, cls)

    #Trading System Manager
    def QueryPortfolioTree(self, portid, version):
        return self.futureaccount.Portfolio.find_one({'portid': portid, 'version': version})

    def InsertRealTimePos(self, pid, gid, iid, p):
        sql = "insert into PortPositionRealTime VALUE (null, '{}', '{}', '{}', '{}', " \
              "0, now())".format(pid, gid, iid, p)
        self.__savedata(sql, self.sqllocpos, self.locppcursor)

    def InsertRiskCapital(self, iid, p):
        sql = "insert into RiskCapital value (null, '{}', '{}', 0)".format(iid, p)
        self.__savedata(sql, self.sqllocss, self.locsscursor)

if __name__ == '__main__':
    print("DatabaseHub init")
    db = DataBaseHub()
    #df = db.QueryDailyCandles('2010-01-04', '2010-01-20')
    #df = db.QueryFactorInfo('ry_yy', '2010-01-04', '2011-01-04')
    #df = db.QuerySinglealphaDaily(190, '20200818')
    #jqdata = JQData()
    #df = jqdata.getStockDailyCandle('600641.XSHG', '2020-08-03', 250)
    #tdays =jqdata.get_trade_days('2020-01-01', '2020-12-31')
    pTree = db.QueryPortfolioTree('Ptest', '1.0')
    print(pTree)
    #pprint(df)
    #sns.distplot(df['normscore'])
    #plt.show()
