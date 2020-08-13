import pymysql
import pymongo
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

    def QuerySingleStockScoreDaily(self, num, code, tday):
        table = "alpha_"+str(num).zfill(3)
        sql = "select * from {} where code = '{}' and TradeDate = '{}'".format(table, code, tday)
        try:
            self.alcursor.execute(sql)
            res = self.alcursor.fetchall()
            df = pd.DataFrame(list(res))
            df.columns = ['code', 'Name', 'TradeDate', 'factor', 'score']
            return df
        except:
            print('Error:QuerySingleStockScoreDaily Unable to fetch data')

    def QueryMultiStocksScoreDaily(self, nums, code, tday):
        dfs = []
        for num in nums:
            dfs.append(self.QuerySingleStockScoreDaily(num, code, tday))
        return pd.concat(dfs)

    def QuerySinglealphaDaily(self, num, tday):
        table = "alpha_"+str(num).zfill(3)
        sql = "select * from {} where TradeDate = {}".format(table, tday)
        try:
            self.alcursor.execute(sql)
            res = self.alcursor.fetchall()
            df = pd.DataFrame(list(res))
            df.columns = ['Code', 'Name', 'TradeDate', 'factor', 'score']
            df['discrete_score'] = pd.qcut(df['score'], q=5, labels=False, precision=10) - 2
            #4，6，44，56，75，101，123
            return df
        except:
            print('Error:QueryalphaDaily Unable to fetch data {}'.format(table))

    def QueryMultialphaDaily(self, nums, tday):
        dfs = []
        for num in nums:
            dfs.append(self.QuerySinglealphaDaily(num, tday))
        return pd.concat(dfs)

    def QueryStockPool(self):
        sql = "select * from stockpool"
        try:
            self.cccursor.execute(sql)
            res = self.cccursor.fetchall()
            df = pd.DataFrame(list(res))
            df.columns = ['Code', 'Name', 'sw_l2', 'jq_l2', 'EntryDate']
            return df
        except:
            print('Error:QueryStockPool Failed')

    def QueryStockRanking(self, table, tday):
        sql = "select * from {} where TradeDate = {}".format(table, tday)
        try:
            self.cccursor.execute(sql)
            res = self.cccursor.fetchall()
            df = pd.DataFrame(list(res))
            df.columns = ['Code', 'Name', 'TradeDate', 'score']
            return df
        except:
            print('Error:QueryStockRanking Unable to fetch data {}'.format(table))

    def SaveScore(self, df, table):
        #df.to_sql(con=self.sqlcc, name='ewscore', if_exists='replace', index=False)
        for index, row in df.iterrows():
            sql = "insert into {} VALUE ('{}', '{}', '{}',{:4f});".format(table, row['Code'], row['Name'], row['TradeDate'], row['score'])
            try:
                self.cccursor.execute(sql)
                self.sqlcc.commit()
            except:
                print('Error:{} Score Failed'.format(table))

    def SaveStockPool(self, code, name, tday, swi, jqi):
        sql = "insert into stockpool VALUE ('{}', '{}', '{}', '{}', '{}') on duplicate key update " \
              "sw_l2 = '{}', jq_l2 = '{}', EntryDate = '{}';".format(code, name, swi, jqi, tday, swi, jqi, tday)
        try:
            self.cccursor.execute(sql)
            self.sqlcc.commit()
        except:
            print('Error:SaveSockPool Failed {} tday = {}'.format(code, tday))


    # Futures
    def SaveContractDailyCandle(self, df):
        sql = "insert into dailycandle VALUE ('{}', '{}', {},{}, {}, {}, {}, {},'{}')" \
            .format(df['Contract'].values[0], df['product'].values[0], df['open'].values[0],
                    df['high'].values[0], df['low'].values[0],  df['close'].values[0],
                    df['volume'].values[0], df['money'].values[0], df['Date'].values[0])
        try:
            self.ctacursor.execute(sql)
            self.sqlcta.commit()
            return True
        except:
            print('Error:SaveContractDailyCandle')
            pprint(df)
            return False

    def QueryDailyProductPrice(self, p, tday):
        sql = "select * from dailycandle where product = '{}' and Date = '{}'".format(p, tday)
        try:
            self.ctacursor.execute(sql)
            res = self.ctacursor.fetchall()
            if len(res) >=2:
                df = pd.DataFrame(list(res))
                df.columns = ['contract', 'product', 'open', 'high', 'low', 'close', 'volume', 'money', 'date']
                return df
            else:
                return None
        except:
            print('Error:QueryDailyProductPrice Unable to fetch data {}'.format(p))
            print(sql)

    def SaveFactorInfo(self, p, dom, tday):
        sql = "insert into factors (product, Date, dominate) VALUE ('{}', '{}', '{}')" .format(p, tday, dom)
        try:
            self.ctacursor.execute(sql)
            self.sqlcta.commit()
            return True
        except:
            print('Error:SaveFactorInfo {}, {}'.format(p, tday))
            return False

    def QueryFactorInfo(self, factor, sdate, edate):
        sql = "select product,Date,dominate,{} from factors where Date >='{}' and Date <= '{}'".format(factor, sdate, edate)
        try:
            self.ctacursor.execute(sql)
            res = self.ctacursor.fetchall()
            if len(res) > 0:
                df = pd.DataFrame(list(res))
                df.columns = ['product', 'date', 'dominate', factor]
                return df
            else:
                return None
        except:
            print('Error:QueryFactorInfo Failed {}'.format(sql))

    def Saverollyield(self, p, tday, ry_abs, ry_yy):
        sql = "update factors set ry_abs = {:4f}, ry_yy = {:4f} where product = '{}' and Date = '{}';".format(ry_abs, ry_yy, p, tday)
        try:
            self.ctacursor.execute(sql)
            self.sqlcta.commit()
            return True
        except:
            print('Error: Saverollyield Failed {}, {}'.format(p, tday))
            return False

    def SaveFutureBasisDaily(self, df):
        sql = "insert into futurebasis VALUE ('{}',{:4f},'{}',{},'{}', {},'{}')" \
            .format(df['product'].values[0], (df['close'].values[0] - df['close'].values[1]), df['contract'].values[0],
                    df['close'].values[0], df['contract'].values[1], df['close'].values[1], df['date'].values[0])
        try:
            self.ctacursor.execute(sql)
            self.sqlcta.commit()
            return True
        except:
            print('Error::SaveFutureBasisDaily')
            pprint(sql)
            return False

    def QueryFutureBasisDaily(self, tday):
        sql = "select * from futurebasis where Date = '{}'".format(tday)
        try:
            self.ctacursor.execute(sql)
            res = self.ctacursor.fetchall()
            if len(res) > 0:
                df = pd.DataFrame(list(res))
                df.columns = ['product', 'basis', 'near', 'nprice', 'far', 'fprice', 'date']
                return df
            else:
                return None
        except:
            print('Error:QueryFutureBasisDaily Failed {}'.format(sql))



if __name__ == '__main__':
    print("DatabaseHub init")
    db = DataBaseHub()
    df = db.QueryFactorInfo('ry_yy', '2010-01-04', '2011-01-04')
    #jqdata = JQData()
    #df = jqdata.getStockDailyCandle('600641.XSHG', '2020-08-03', 250)
    pprint(df)
    #sns.distplot(df['normscore'])
    #plt.show()
