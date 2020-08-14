import DataBaseHub
import datetime
from pprint import pprint


class calRank:
    def __init__(self):
        self.db = DataBaseHub.DataBaseHub()

    def ewscore(self, tday):
        factors = [f for f in range(1, 191) if f not in [4, 6, 9, 44, 53, 56, 75, 86, 101, 103, 123,
                                                         140, 144, 148, 149, 154, 171, 177, 181, 182, 183]]
        df = self.db.QueryMultialphaDaily(factors, tday)
        df = df.drop(columns=['score'])
        df_score = df.groupby(['Code', 'Name']).mean()
        df_score['TradeDate'] = tday
        df_score.columns = ['score', 'TradeDate']
        df_score.reset_index(level=['Code', 'Name'], inplace=True)
        pprint(df_score.head())
        self.db.SaveScore(df_score, 'ewscore')

    def stockpoolranking(self, scores, tday):
        df = self.db.QueryStockRanking(scores, tday)
        stockpool = self.db.QueryStockPool()['Code']
        df_pool = df.loc[df['Code'].isin(stockpool)]
        self.db.SaveScore(df_pool, 'spewscore')


if __name__ == '__main__':
    print("calScore init")
    cal = calRank()
    tday = '20200813'
    cal.ewscore(tday)
    cal.stockpoolranking('ewscore', tday)
