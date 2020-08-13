import Utility
import logging
import DataBaseHub
from PortfolioManager import *

pmlogger = logging.getLogger(__name__)


class FutureBackTest:
    def __init__(self, factor):
        self.db = DataBaseHub.DataBaseHub()
        self.jqdata = DataBaseHub.JQData()
        self.port = Portfolio()
        self.factor = factor


    def run(self, sdate, edate):
        self.ftable = self.db.QueryFactorInfo(self.factor, sdate, edate)
        pass

    def onMarketOpen(self):
        pass

    def onMarketClose(self):
        pass

    def _getMarketData(self):
        pass

    def summary(self):
        pass

    def calTargetPosition(self, tday):
        pass


    def _carryv1(self, tday):
        pass


if __name__ == '__main__':
    pass