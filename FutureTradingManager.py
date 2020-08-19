from DataBaseHub import DataBaseHub

class TradingSystemManager:
    def __init__(self):
        self.db = DataBaseHub()

    def updatePortDB(self, portid, version):
        pTree = self.db.QueryPortfolioTree(portid, version)
        for g, group in enumerate(pTree['groups']):
            for i, inst in enumerate(group['insts']):
                self.db.InsertRealTimePos(portid, inst['groupid'], inst['instanceid'], inst['productid'])
                self.db.InsertRiskCapital(inst['instanceid'], inst['productid'])


if __name__ == '__main__':
    tsm = TradingSystemManager()
    tsm.updatePortDB('Ptest', '1.0')