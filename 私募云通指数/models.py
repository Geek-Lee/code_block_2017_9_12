# from django.db import models
# from api.settings import DATABASES
from sqlalchemy import create_engine
import datetime as dt
import pandas as pd
# from self_management.models_detail import fetch_market_index, fetch_pe_index, format_sql_value, format_sql_columns
from dateutil.relativedelta import relativedelta

# Create your models here.
_benchmarks = ['hs300', 'csi500', 'sse50', 'cbi', 'nfi']
#引擎
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

#一年前的时间，现在的时间，
def year_now():
    now = dt.datetime.now().date()
    return {'min': (now - relativedelta(years=1)).strftime('%Y-%m-%d'), 'max': now.strftime("%Y-%m-%d")}


class PeIndex:
    """
    私募云通指数实例
    """

    def __init__(self, index_id: list or str, benchmarks: list or str, date_range: dict or None):
        """
        #参数：索引id， 基准
        Parameters#
        ----------
        index_id
        benchmarks
        date_range
        """
        self.bm = benchmarks
        self.ind = index_id if isinstance(index_id, list) else [index_id]
        self.date = date_range if date_range else year_now()

    #interval间隔
    '''
    sql选出index_id,max,min从fund_month_index WHERE index_id在【】，index_id分组,返回字典：最大时间最小时间
    '''
    def interval(self):
        sql = "SELECT index_id, min(statistic_date) as `min`, max(statistic_date) as `max` FROM fund_month_index " \
              "WHERE index_id in ({}) GROUP BY index_id".format(format_sql_value(self.ind))
        temp = pd.read_sql(sql, engine_rd)
        return {'min': temp['min'].min().strftime("%Y-%m-%d"), 'max': temp['max'].max().strftime("%Y-%m-%d")}

    #值
    '''
    if total ,drange为一年的间隔【字典】，否则自定义的日期【字典】
    sql: 选出index_id, statistic_date, index_value  FROM fund_month_index ，其中index_id在{列表}，statistic_date在最大最小之间。
    df.set_index(['data1','data2']).unstack(level=1)
    Out[22]:
           data3
    data2     11     22     33      44
    data1
    1      111.0    NaN    NaN     NaN
    2        NaN  222.0    NaN     NaN
    3        NaN    NaN  333.0     NaN
    4        NaN    NaN    NaN  4444.0

    df1.columns = df1.columns.levels[1].values
    df1
    Out[29]:
              11     22     33      44
    data1
    1      111.0    NaN    NaN     NaN
    2        NaN  222.0    NaN     NaN
    3        NaN    NaN  333.0     NaN
    4        NaN    NaN    NaN  4444.0

    '''


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

    # 市场指数
    '''
    取出市场指数，index为statistic_date，排序
    索引变格式，月结束频率，最后一个月的。
    '''

    def market_index(self, total):
        if total:
            drange = self.interval()
        else:
            drange = self.date()
        result = fetch_market_index(self.bm, drange, engine=engine_rd).set_index('statistic_date').sort_index()
        result.index = pd.DatetimeIndex(result.index)
        result = result.resample('M').last()
        return result

    # 拼接
    '''
    值和市场指数的df拼接，去空
    '''
    def data(self, total=False):
        result = pd.concat([self.values(total), self.market_index(total)], axis=1).dropna(subset=self.ind, how='all')
        return result

    #累计收益
    def accumulate_return(self):
        indexs = self.data()
        result = indexs / indexs.iloc[0, :] - 1
        return result

    #月度收益
    def month_return(self):
        indexs = self.data()
        result = indexs / indexs.shift(1) - 1
        return result

    #动态系数
    '''
    data为拼接的dataframe【间隔】，最后23行，删除index_id
    '''
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
        datas = pe.data(total=True).loc[date_range['min']: date_range['max'], :]
        result = datas.corr() # datas自己和自己每列的相关系数
        return result, date_interval

#取出市场指数
'''
选择基准指数，statistic_date FROM market_index 在statistic_date的一个区间中，返回字典
'''
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
