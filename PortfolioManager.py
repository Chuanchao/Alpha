import Utility
import logging
from enum import Enum

pmlogger = logging.getLogger(__name__)


class Direction(Enum):
    LONG, SHORT = 1, -1


class Position:
    def __init__(self, product, contract, direction):
        self.product = product
        self.contract = contract
        self.directon = direction
        self.cost = 0
        self.vol = 0

    def OrderTargetAmount(self, amount, contract, price):
        if self.contract != contract or price <= 0:
            return 0
        targetvol = int(amount/price)
        deltavol = targetvol - self.vol
        if deltavol >= 0:
            self.cost = (self.cost*self.vol + price*deltavol)/targetvol
            self.vol = targetvol
            return 0
        if deltavol < 0:
            self.vol = targetvol
            return self.directon*(price - self.cost)*deltavol

    def calpnl(self, contract, price):
        if contract != self.contract:
            return 0
        return self.directon*(price-self.cost)*self.vol

    def getvol(self):
        return self.vol


class Portfolio:
    def __init__(self):
        self.poslong = {}
        self.posshort = {}
        self.pnl = 0
        self.pnls = {}

    def getlongcontracts(self):
        return list(self.poslong)

    def getshortcontracts(self):
        return list(self.posshort)

    def OrderTargetAmount(self, amount, product, contract, price, direction):
        if direction == Direction.LONG:
            self.__addlongpos(product, contract)
            pnl = self.poslong[contract].OrderTargetAmount(amount, contract, price)
            self.pnl = self.pnl + pnl
            if self.poslong[contract].getvol() == 0:
                del self.poslong[contract]

        if direction == Direction.SHORT:
            self.__addshortpos(product, contract)
            pnl = self.posshort[contract].OrderTargetAmount(amount, contract, price)
            self.pnl = self.pnl + pnl
            if self.posshort[contract].getvol() == 0:
                del self.posshort[contract]

    def calpnl(self, tday, df):
        pnl = 0
        for contract in self.poslong:
            close = df.loc[df['contract'] == contract]['close']
            pnl = pnl + self.poslong['contract'].calpnl(contract, close)

        for contract in self.posshort:
            close = df.loc[df['contract'] == contract]['close']
            pnl = pnl + self.posshort['contract'].calpnl(contract, close)

        self.pnls[tday] = self.pnl + pnl

    def __addlongpos(self, product, contract):
        if contract not in self.poslong:
            self.poslong[contract] = Position(product, contract, Direction.LONG)

    def __addshortpos(self, product, contract):
        if contract not in self.posshort:
            self.posshort[contract] = Position(product, contract, Direction.LONG)


if __name__ == '__main__':
    pass