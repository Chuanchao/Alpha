from DataBaseHub import DataBaseHub
from DataBaseHub import JQData
import Utility
import logging
import datetime as dt
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None

calogger = logging.getLogger(__name__)


class CTAlpha:
    def __init__(self):
        self.jqdata = JQData()
        self.db = DataBaseHub()
        self.products = ['au', 'ag', 'cu', 'al', 'zn', 'pb', 'ni', 'sn', 'rb', 'i', 'hc',
                         'ss', 'SF', 'SM', 'jm', 'j', 'FG', 'sp', 'fu', 'lu', 'sc', 'bu',
                         'pg', 'ru', 'nr', 'l', 'TA', 'v', 'eg', 'MA', 'pp', 'eb', 'UR',
                         'SA', 'c', 'a', 'cs', 'b', 'm', 'y', 'RM', 'OI', 'p', 'CF', 'SR',
                         'jd', 'AP', 'CJ']
        self.upp = [p.upper() for p in self.products]

    #Basic Data Update
    def UpdateDailyCandle(self, tday):
        for p in self.upp:
            contracts = self.jqdata.get_future_contracts(p, tday)
            for c in contracts:
                df = self.jqdata.getContractDailyCandle(c, p, tday, 1)
                self.db.SaveContractDailyCandle(df)

    def UpdateFactorInfo(self, tday):
        for p in self.upp:
            dom = self.jqdata.getDominantFuture(p, tday)
            if len(dom) > 0:
                self.db.SaveFactorInfo(p, dom, tday)


    def UpdateFutureBasis(self, tday):
        for p in self.upp:
            df = self.db.QueryDailyProductPrice(p, tday)
            if df is not None:
                df = df.sort_values(by=['volume'], ascending=False).iloc[:2]
                df.sort_values(by=['contract'], ascending=True, inplace=True)
                self.db.SaveFutureBasisDaily(df)

    def UpdateHistroyData(self, sdate, edate):
        for d in self.jqdata.get_trade_days(sdate, edate):
            #self.UpdateDailyCandle(d)
            #self.UpdateFutureBasis(d)
            #self.UpdateFactorInfo(d)
            self.calrollyield(d)

    def calrollyield(self, tday):
        basis = self.db.QueryFutureBasisDaily(tday)
        for id, row in basis.iterrows():
            try:
                ry_abs = row['basis']/row['fprice']
                ndate = self.jqdata.getedate(row['near'])
                fdate = self.jqdata.getedate(row['far'])
                delta = (fdate - ndate).days
                ry_yy = ry_abs*365/delta
                self.db.Saverollyield(row['product'], row['date'], ry_abs, ry_yy)
            except:
                calogger.error("%s %s %s %d", row['product'], ndate, fdate, delta)


if __name__ == '__main__':
    ca = CTAlpha()
    #tday = dt.datetime.date.today().strftime('%Y%m%d')
    #ca.UpdateFactorInfo('2010-01-04')
    ca.UpdateDailyCandle('2020-07-22')
    #ca.UpdateFutureBasis('2020-07-21')
    #ca.UpdateHistroyData('2010-01-04', '2020-07-21')
