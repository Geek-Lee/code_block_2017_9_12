# Create your views here.
from API_V1.models import *
import datetime as dt
from utils.DateFunc import month_day, date_str
from utils.type_code import code_trans, sort_code
from django.views.decorators.csrf import csrf_exempt
import pandas as pd
from django.shortcuts import render, render_to_response
from django.template import RequestContext
# from django.http import request
# from rest_framework import request
from rest_framework import permissions, status
from rest_framework.response import Response
from rest_framework.decorators import api_view, permission_classes
import json

from sqlalchemy import create_engine
from api.settings import DATABASES

from utils.api_test.api_test import benchmark_fund_indicator

engine_rd = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(
    DATABASES['default']['USER'],
    DATABASES['default']['PASSWORD'],
    DATABASES['default']['HOST'],
    DATABASES['default']['PORT'],
    DATABASES['default']['NAME'],
), connect_args={"charset": "utf8"})


@api_view(http_method_names=['GET', 'POST'])
@permission_classes((permissions.AllowAny,))
def series_record(request):
    """
    净值序列
    :param fund_id: 基金产品ID
    :param benchmarks: optional, benchmark列表
    :param date_start: optional, 统计区间起始日期，默认为空字符串
    :param date_end: optional, 统计区间结束日期，默认为空字符串 
    :return: 
    """

    fund_id = request.data['fund_id']
    date_start = request.data['date_start'] if 'date_start' in request.data else None
    date_end = request.data['date_end'] if 'date_end' in request.data else None
    if date_start is not None and len(date_start) == 0:
        date_start = None
    if date_end is not None and len(date_end) == 0:
        date_end = None

    benchmarks = request.data['benchmarks'].split(',') if 'benchmarks' in request.data else None

    try:
        origina_data = get_series_data(fund_id, benchmarks, date_start, date_end)
        data = origina_data.fillna('-')  # 空值替换成'-'
        # if len(data) == 0:
        #     return Response( json.dumps({'msg': '数据为空'}), status=status.HTTP_200_OK)
        print(data)
        # data.sort_index(ascending=False, inplace=True)
        data.sort_values(by="statistic_date", ascending=False, inplace=True)
        date_list = list(map(date_str, data['statistic_date'].tolist()))
        data = data.drop('statistic_date', axis=1)
        data_list = [{'data': data[col].tolist(), 'name': col} for col in data.columns.tolist()]

        # 插值数据
        df_interpolate = origina_data.interpolate()  # 对空值进行线性插值
        df_interpolate = df_interpolate.drop('statistic_date', axis=1)
        print(df_interpolate.head())

        # 累计收益率计算
        df_interpolate['return_nav'] = df_interpolate['swanav']/df_interpolate['swanav'].tolist()[0] - 1
        if df_interpolate['return_nav'].isnull().all():
            df_interpolate['return_nav'] = df_interpolate['added_nav'] / df_interpolate['added_nav'].tolist()[0] - 1
        if df_interpolate['return_nav'].isnull().all():
            df_interpolate['return_nav'] = df_interpolate['nav'] / df_interpolate['nav'].tolist()[0] - 1
        print(df_interpolate['return_nav'].head())

        indexs = ['hs300', 'sse50', 'csi500', 'cbi', 'nfi']
        for ix in indexs:
            if ix in df_interpolate.columns:
                init_data = df_interpolate[ix].tolist()[0]
                for i in range(len(df_interpolate[ix].tolist())):
                    if str(df_interpolate[ix].tolist()[i]) != 'nan':
                        init_data = df_interpolate[ix].tolist()[i]
                        break

                df_interpolate['return_{}'.format(ix)] = df_interpolate[ix] / init_data - 1
                df_interpolate['return_{}'.format(ix)][:i] = 0

        df_interpolate = df_interpolate.fillna('-')
        data_interpolate_list = [{'data': df_interpolate[col].tolist(), 'name': col} for col in
                                 df_interpolate.columns.tolist()]
        print(df_interpolate.head())

        resp_data = {'dates_reverse': date_list, 'dates': date_list[::-1], 'data': data_list, 'data_interpolate': data_interpolate_list}
        resp_data = json.dumps(resp_data)
        return Response(resp_data)
    except (TypeError, ValueError) as e:
        error_log = json.dumps({'error_log': '请检查参数'})
        return Response(error_log, status.HTTP_400_BAD_REQUEST)


def get_series_data(fund_id, benchmarks=None, date_start='', date_end=''):


    # benchmark = kwargs.get('benchmark')  # [hs300, sse50, ssia, cbi, nfi]
    if benchmarks is None or (len(benchmarks) == 1 and benchmarks[0] == ''):
        benchmarks = ['hs300', 'sse50', 'csi500', 'cbi', 'nfi']

    table, bm_table = 'fund_nv_data', 'market_index'
    chosen_col = ['nav', 'added_nav', 'swanav']
    if date_start is None or date_end is None:
        sql_date = "SELECT MAX(statistic_date) AS max_d, MIN(statistic_date) as min_d " \
                   "FROM {} WHERE fund_id = '{}'".format(table, fund_id)
        dates = pd.read_sql(sql_date, engine_rd).to_dict(orient='record')[0]
        if date_start is None:
            date_start = date_str(dates['min_d'])
        if date_end is None:
            date_end = date_str(dates['max_d'])

    # if benchmark is not None:
    col_bm = format_col(benchmarks, bm_table)# 基准指数
    col_chosen = format_col(chosen_col, table)# 选择的指数
    sql_left = "SELECT {col_chosen},{col_bm},{table}.statistic_date FROM {table} " \
               "LEFT JOIN {bm_table} ON {table}.statistic_date = {bm_table}.statistic_date" \
               " WHERE {table}.fund_id = '{fund_id}' " \
               "AND {table}.statistic_date BETWEEN '{date_start}' AND '{date_end}'".format(col_chosen=col_chosen,
                                                                                           col_bm=col_bm, table=table,
                                                                                           bm_table=bm_table,
                                                                                           fund_id=fund_id,
                                                                                           date_start=date_start,
                                                                                           date_end=date_end)
    sql_right = "SELECT {col_chosen},{col_bm},{bm_table}.statistic_date FROM {bm_table}" \
                " LEFT JOIN {table} ON {table}.statistic_date = {bm_table}.statistic_date " \
                "AND {table}.fund_id = '{fund_id}'" \
                " WHERE {bm_table}.statistic_date BETWEEN '{date_start}' AND '{date_end}'".format(col_chosen=col_chosen,
                                                                                                  col_bm=col_bm,
                                                                                                  bm_table=bm_table,
                                                                                                  table=table,
                                                                                                  fund_id=fund_id,
                                                                                                  date_start=date_start,
                                                                                                  date_end=date_end)
    sql = sql_left + " UNION " + sql_right
    data = pd.read_sql(sql, engine_rd)
    return data

    # table_col_match = {
    #     'nav': 'fund_nv_data' + ',' + 'market_index',
    #     'added_nav': 'fund_nv_data' + ',' + 'market_index',
    #     'swanav': 'fund_nv_data' + ',' + 'market_index',
    # }
    # sql_product = "SELECT {},statistic_date FROM {}" \
    #               " WHERE fund_id = '{}' AND statistic_date BETWEEN '{}' AND '{}'".format(','.join(chosen_col),
    #                                                                                       table, fund_id,
    #                                                                                       date_start, date_end)
    # sql_bm = "SELECT {},statistic_date FROM {}" \
    #          " WHERE statistic_date BETWEEN '{}' AND '{}'".format(','.join(benchmark), bm_table,
    #                                                               date_start, date_end)
    # data = pd.read_sql(sql_product, engine_rd)
    # bm_data = pd.read_sql(sql_bm, engine_rd)
    # df = pd.concat([data, bm_data], axis=1, join='outer')
    # benchmark = ['hs300', 'sse50', 'ssia', 'cbi', 'nfi']
    # fund_id = 'JR000001'
    # date_start = '20160520'
    # date_end = '20170424'
    # table, bm_table = 'fund_nv_data', 'market_index'
    # chosen_col = ['nav', 'added_nav', 'swanav']


@api_view(http_method_names=['GET', 'POST'])
@permission_classes((permissions.AllowAny,))
def return_and_mdd(request):
    fund_id = request.data['fund_id']
    date_start = request.data['date_start'] if 'date_start' in request.data else None
    date_end = request.data['date_end'] if 'date_end' in request.data else None
    if date_start is not None and len(date_start) == 0:
        date_start = None
    if date_end is not None and len(date_end) == 0:
        date_end = None

    benchmarks = request.data['benchmarks'].split(',') if 'benchmarks' in request.data else None
    col = request.data['col']
    freq = request.data['freq']
    try:
        resp_data = get_return_mdd(fund_id=fund_id, col=col, freq=freq, benchmarks=benchmarks, date_start=date_start,
                                   date_end=date_end)
        resp_data = json.dumps(resp_data)
        return Response(resp_data)
    except (TypeError, ValueError) as e:
        return Response({'error': e}, status=status.HTTP_400_BAD_REQUEST)


def get_return_mdd(fund_id, col, freq, benchmarks=None, date_start=None, date_end=None, **kwargs):
    """
    
    :param fund_id: 
    :param benchmarks: 
    :param col: return/mdd
    :param freq: 
    :param freq_lenth: optional
    :param date_start: 
    :param date_end: 
    :param kwargs: 
    :return: 
    
    """
    if benchmarks is None or (len(benchmarks) == 1 and benchmarks[0] == ''):
        benchmarks = ['hs300', 'sse50', 'csi500', 'cbi', 'nfi']
    if isinstance(benchmarks, list):
        benchmarks = list(map(lambda x: x.upper(), benchmarks))
    if isinstance(benchmarks, str):
        benchmarks = benchmarks.upper()
    bm_table = "index_{}_table".format(freq)
    freq_lenth = ["m3", "m6", 'y1', "y2", "y3", "y5", "total"]
    if col == "return":
        chosen_col = ["{}_return".format(x) for x in freq_lenth]
        table = "fund_{}_return".format(freq)
    else:
        chosen_col = ["{}_max_retracement".format(x) for x in freq_lenth]
        table = "fund_{}_risk".format(freq)
    if date_start is None or date_end is None:
        sql_date = "SELECT MAX(statistic_date) AS max_d, MIN(statistic_date) as min_d " \
                   "FROM {} WHERE fund_id = '{}'".format(table, fund_id)
        dates = pd.read_sql(sql_date, engine_rd).to_dict(orient='record')[0]
        if date_start is None:
            date_start = date_str(dates['min_d'])
        if date_end is None:
            date_end = date_str(dates['max_d'])

    sql = "select {col},fund_name,statistic_date from {tb} " \
          "WHERE fund_id ='{fund_id}' AND  statistic_date BETWEEN '{ds}' AND '{de}' AND benchmark=1".format(
        col=format_col(chosen_col, table), tb=table, fund_id=fund_id, ds=date_start, de=date_end)
    df_data = pd.read_sql(sql, engine_rd)
    sql_bm = "select {col},index_id,statistic_date from {tb} " \
             "WHERE index_id in ({bms}) AND  statistic_date BETWEEN '{ds}' AND '{de}'".format(
        col=format_col(chosen_col, bm_table), tb=bm_table, bms=format_value(benchmarks), ds=date_start, de=date_end)
    df_bm = pd.read_sql(sql_bm, engine_rd)
    data_dict = {}
    for i in range(len(chosen_col)):
        col = chosen_col[i]
        bm_data = df_bm[['index_id', 'statistic_date', col]]
        data = df_data[col]
        data.index = df_data['statistic_date'].tolist()
        data.name = df_data['fund_name'][0]
        bm_data_list = [data]
        for bm in benchmarks:
            each_bm_data = bm_data[bm_data['index_id'] == bm]
            each_bm_data.index = each_bm_data['statistic_date'].tolist()
            each_bm_data = each_bm_data.drop(['statistic_date', 'index_id'], axis=1)
            each_bm_data.rename(columns={col: bm}, inplace=True)
            bm_data_list.append(each_bm_data)
        col_data_df = pd.concat(bm_data_list, axis=1)

        # 插值数据
        col_data_df_interpolate = col_data_df.interpolate()  # 对空值进行线性插值
        col_data_df_interpolate = col_data_df_interpolate.fillna('-')

        col_data = {'dates': list(map(date_str, col_data_df.index.tolist())),
                    "data": [{"name": cn, "data": col_data_df[cn].tolist()} for cn in col_data_df.columns],
                    "data_interpolate": [{"name": cn, "data": col_data_df_interpolate[cn].tolist()} for cn in
                                         col_data_df_interpolate.columns]}

        data_dict[freq_lenth[i]] = col_data
    return data_dict


def format_col(col_list, table_name):
    if isinstance(col_list, list):
        col = ['{}.'.format(table_name) + x for x in col_list]
        return ','.join(col)
    elif isinstance(col_list, str):
        return '{}.{}'.format(table_name, col_list)


def format_list(col_list, freq_lenth):
    return list(map(lambda x: x.format(freq_lenth), col_list))


def format_value(value):
    if isinstance(value, list):
        return ",".join(map(lambda x: "'{}'".format(x), value))
    elif isinstance(value, str):
        return "'{}'".format(value)


def rename_col(df):
    df.columns = df.loc['name'].tolist()
    return df.drop('name')
