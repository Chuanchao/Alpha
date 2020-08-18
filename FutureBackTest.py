import Utility
import logging
import DataBaseHub
from PortfolioManager import *

pmlogger = logging.getLogger(__name__)


class FutureBackTest:
    def __init__(self):
        self.db = DataBaseHub.DataBaseHub()
        self.jqdata = DataBaseHub.JQData()
        self.port = Portfolio()


    def run(self, factor, sdate, edate):
        self.mkdata = self.db.QueryDailyCandles(sdate, edate)
        self.ftable = self.db.QueryFactorInfo(factor, sdate, edate)
        self.tdays = [d.strftime('%Y-%m-%d') for d in self.jqdata.get_trade_days(sdate, edate)]
        for td in self.tdays:
            tdata = self.mkdata.loc[self.mkdata['date'] == td]

    def onMarketOpen(self, data):
        pass

    def onMarketClose(self, data):
        pass

    def TodomContract(self):
        pass

    def _calranking(self, sdate, edate):
        pass

    def summary(self):
        pass

    def calTargetPosition(self, tday):
        pass


    def _carryv1(self, tday):
        pass


if __name__ == '__main__':
    fbt = FutureBackTest()
    fbt.run('ry_yy', '2010-01-04', '2010-01-10')