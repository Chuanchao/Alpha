from DataBaseHub import DataBaseHub
from DataBaseHub import JQData
import datetime
import pandas as pd

class StockPool:
    def __init__(self):
        self.jqdata = JQData()
        self.db = DataBaseHub()

    def stockfilter(self, lb, win):
        pass

    def AddStock(self, slist, tday):
        for s in self.jqdata.normalizecode(slist):
            try:
                name = self.jqdata.getstockname(s)
                industry = self.jqdata.getindustry(s, tday)
                sw_l2 = industry[s]['sw_l2']['industry_name']
                jq_l2 = industry[s]['jq_l2']['industry_name']
                self.db.SaveStockPool(s, name, tday, sw_l2, jq_l2)
            except:
                print('Error:AddStock Failed at {}'.format(s))

    def RemoveStock(self):
        pass


if __name__ == '__main__':
    sp = StockPool()
    #slist = []
    slist = pd.read_csv('stockpool/stockpool.txt')['code'].tolist()
    tday = datetime.date.today().strftime('%Y-%m-%d')
    sp.AddStock(slist, tday)
