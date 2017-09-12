# from django.db import models
# from api.settings import DATABASES
from sqlalchemy import create_engine
import datetime as dt
import pandas as pd
# from self_management.models_detail import fetch_market_index, fetch_pe_index, format_sql_value, format_sql_columns
from dateutil.relativedelta import relativedelta

# Create your models here.
_benchmarks = ['hs300', 'csi500', 'sse50', 'cbi', 'nfi']
db_setting = {
    'ENGINE': 'django.db.backends.mysql',
    'NAME': 'test_subsidiary',
    'USER': 'sm01',
    'PASSWORD': 'x6B28Vz9',
    'HOST': '182.254.128.241',
    'PORT': 8612,
}
engine_rd = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(
    db_setting['USER'],
    db_setting['PASSWORD'],
    db_setting['HOST'],
    db_setting['PORT'],
    db_setting['NAME'],
), connect_args={"charset": "utf8"})


def year_now():
    now = dt.datetime.now().date()
    return {'min': (now - relativedelta(years=1)).strftime('%Y-%m-%d'), 'max': now.strftime("%Y-%m-%d")}


class PeIndex:
    """
    私募云通指数实例
    """

    def __init__(self, index_id: list or str, benchmarks: list or str, date_range: dict or None):
        """
        Parameters
        ----------
        index_id
        benchmarks
        date_range
        """
        self.bm = benchmarks
        self.ind = index_id if isinstance(index_id, list) else [index_id]
        self.date = date_range if date_range else year_now()

    def interval(self):
        sql = "SELECT index_id, min(statistic_date) as `min`, max(statistic_date) as `max` FROM fund_month_index " \
              "WHERE index_id in ({}) GROUP BY index_id".format(format_sql_value(self.ind))
        temp = pd.read_sql(sql, engine_rd)
        return {'min': temp['min'].min().strftime("%Y-%m-%d"), 'max': temp['max'].max().strftime("%Y-%m-%d")}

    def values(self, total=False):
        if total:
            drange = self.interval()
        else:
            drange = self.date()
        sql = "SELECT index_id, statistic_date, index_value  FROM fund_month_index " \
              "WHERE index_id IN ({}) AND statistic_date BETWEEN '{}' AND '{}'" \
              "".format(format_sql_value(self.ind), drange['min'], drange['max'])
        result = pd.read_sql(sql, engine_rd).set_index(['statistic_date', 'index_id']).unstack(level=1)
        result.columns = result.columns.levels[1].values
        return result

    def market_index(self, total):
        if total:
            drange = self.interval()
        else:
            drange = self.date()
        result = fetch_market_index(self.bm, drange, engine=engine_rd).set_index('statistic_date').sort_index()
        result.index = pd.DatetimeIndex(result.index)
        result = result.resample('M').last()
        return result

    def data(self, total=False):
        result = pd.concat([self.values(total), self.market_index(total)], axis=1).dropna(subset=self.ind, how='all')
        return result

    def accumulate_return(self):
        indexs = self.data()
        result = indexs / indexs.iloc[0, :] - 1
        return result

    def month_return(self):
        indexs = self.data()
        result = indexs / indexs.shift(1) - 1
        return result

    @staticmethod
    def dynamic_coefficient(index_id: str):
        pe = PeIndex(index_id, benchmarks=_benchmarks, date_range=year_now())
        datas = pe.data(total=True).iloc[-23:, :]
        pe_data = pd.DataFrame(datas.pop(index_id))
        df_corr = datas.rolling(window=12).corr(pairwise=True, other=pe_data).iloc[11:, :, 0].T
        return df_corr

    @staticmethod
    def correlation_coefficient(index_id: str, date_range: dict):
        # _freq_map = {'m6': 5, 'y1': 11, 'y2': 23}
        pe = PeIndex(index_id, benchmarks=_benchmarks, date_range=year_now())
        date_interval = pe.interval()
        # date_e = dt.datetime.strptime(date_interval['max'], "%Y-%m-%d").date()
        # date_min = dt.datetime.strptime(date_interval['min'], "%Y-%m-%d").date()
        # date_s = date_e - relativedelta(months=_freq_map.get(freq, 5))
        datas = pe.data(total=True).loc[
                date_range['min']: date_range['max'], :]
        result = datas.corr()
        return result, date_interval


def fetch_market_index(benchmarks, date_range, engine):
    ids = format_sql_columns(benchmarks)
    sql = "SELECT {ids}, statistic_date FROM market_index " \
          "WHERE statistic_date <= '{max_sd}' " \
          "AND statistic_date >='{min_sd}' ORDER BY statistic_date DESC".format(ids=ids, max_sd=date_range['max'],
                                                                                min_sd=date_range['min'])
    return pd.read_sql(sql, engine)


def format_sql_value(value):
    if isinstance(value, list):
        return ",".join(map(lambda x: "'{}'".format(x), value))
    elif isinstance(value, str):
        return "'{}'".format(value)


def format_sql_columns(col, table=None):
    if table is not None:
        if isinstance(col, str):
            return '{}.{}'.format(table, col)
        elif isinstance(col, list):
            return ','.join(map(lambda x: '{}.{}'.format(table, x), col))
    else:
        if isinstance(col, str):
            return col
        elif isinstance(col, list):
            return ','.join(map(lambda x: '{}'.format(x), col))
