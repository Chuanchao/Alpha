import logging
from logging.handlers import TimedRotatingFileHandler

def ConfigLogger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    fh = TimedRotatingFileHandler('log/routine.log', 'D', 1, 30)
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)

    formatter = logging.Formatter("%(asctime)s - [%(levelname)s] [%(threadName)s] (%(module)s:%(lineno)d) %(message)s")

    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

def singleton(class_):
    instances = {}

    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance


def scorenorm(df):
    mean = df['score'].mean()
    std = df['score'].std()
    df['normscore'] = (df['score']-mean)/std


ConfigLogger()

if __name__ == '__main__':
    logging.error('hello')