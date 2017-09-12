# coding:utf-8

"""
国泰君安数据库同步
"""

import sys
import platform

if platform.system() == "Linux":
    sys.path.append("/mydata/code/sync_center")

import numpy as np
import pandas as pd
import multiprocessing
import datetime

from db_config import engine_2g_base, engine_1g_custom_data


def sync_2g_to_4g_product_small():
    """
    # 对于小数据集
    :return:
    """
    """
    # 对于小数据集
    :return:
    """
    engine_source = engine_2g_base
    engine_target = engine_1g_custom_data

    tables_smallsets = [
        'fund_info',
        'fund_info_aggregation',
        'fund_info_subsidiary',
        'fund_org_mapping',
        'fund_manager_mapping',
        'fund_type_mapping',
        'market_index',

        'fund_asset_scale',
        'fund_weekly_index',
        'fund_month_index',
        'manager_info',

        'org_info',
        'org_month_return',
        'org_month_risk',
        'org_month_research',
        'org_month_routine',
        'index_month_table',
        'index_weekly_table',
        'time_index',
    ]

    # 先清空表,再全部写入
    for table in tables_smallsets:
        sql = "select count(*) as cnt from {}".format(table)
        df1 = pd.read_sql(sql, engine_source)
        df4 = pd.read_sql(sql, engine_target)
        count1 = df1['cnt'].get_values()[0]
        count4 = df4['cnt'].get_values()[0]
        print "{} 源表:{}条; 目标表:{}条".format(table, count1, count4)
        if count1 == 0:
            continue

        # 清空目标表内容
        sql_del = "delete from {}".format(table)
        engine_target.execute(sql_del)

        # 全部写入
        step = 10000
        start = 0
        count_now = 0

        for i in range(start, count1, step):
            sql = "select * from {} limit {}, {}".format(table, i, step)
            print sql
            df = pd.read_sql(sql, engine_source)
            df = df.fillna(np.NaN)
            count_now += len(df)
            print "{}: {}/{}".format(table, count_now, count1)
            df.to_sql(table, engine_target, if_exists='append', index=False)


def sync_2g_to_4g_product_big():
    """
    # 对于大数据集
    :return:
    """

    tables_bigsets = {
        'fund_nv_data_standard': 'fund_nv_data',  # name to : 'fund_nv_data'

        'fund_weekly_indicator': 'fund_weekly_indicator',
        'fund_weekly_return': 'fund_weekly_return',
        'fund_weekly_risk': 'fund_weekly_risk',
        'fund_subsidiary_weekly_index': 'fund_subsidiary_weekly_index',

        'fund_month_indicator': 'fund_month_indicator',
        'fund_month_return': 'fund_month_return',
        'fund_month_risk': 'fund_month_risk',
        'fund_subsidiary_month_index': 'fund_subsidiary_month_index',
    }

    tbs = []
    for key in tables_bigsets:
        tbs.append("{}:{}".format(key, tables_bigsets[key]))

    pool = multiprocessing.Pool(processes=4)
    pool.map(_sync_big_helper, tbs)
    pool.close()
    pool.join()


def _sync_big_helper(table_dict):
    engine_source = engine_2g_base
    engine_target = engine_1g_custom_data

    # 增量更新
    table_source = table_dict.split(':')[0]
    table_target = table_dict.split(':')[1]

    sql = "select distinct fund_id from {}".format(table_source)
    df_ids = pd.read_sql(sql, engine_source)
    # 按基金id分别读取
    for fund_id in df_ids['fund_id'].get_values():
        sql_target = "select max(update_time) as mut from {} where fund_id='{}'".format(table_target, fund_id)
        df_max_utime = pd.read_sql(sql_target, engine_target)
        max_update_time = df_max_utime['mut'].get_values()[0]
        if max_update_time != None:
            max_update_time = np.datetime64(max_update_time, 's').astype(datetime.datetime)
            sql = "select * from {} where fund_id='{}' and update_time > '{}'".format(table_source, fund_id,
                                                                                      str(max_update_time))
        else:
            sql = "select * from {} where fund_id='{}' ".format(table_source, fund_id)
        print sql
        df = pd.read_sql(sql, engine_source)
        df = df.fillna(np.NaN)
        print table_target, fund_id, len(df)
        try:
            df.to_sql(table_target, engine_target, if_exists='append', index=False)
            print 'done'
        except Exception, e:
            print '数据集中部分数据主键有重复,不能执行insert, 需要update'
            # 转逐条写入
            for i in range(len(df)):
                df_each = df[i:i + 1]

                try:
                    # 无重复主键
                    df_each.to_sql(table_target, engine_target, if_exists='append', index=False)
                except:
                    # 有重复主键
                    if table_target == "fund_nv_data":
                        sql_del = "delete from {} where fund_id='{}' and statistic_date='{}'".format(
                            table_target,
                            df_each['fund_id'].get_values()[0],
                            df_each['statistic_date'].get_values()[0])
                    else:
                        sql_del = "delete from {} where fund_id='{}' and statistic_date='{}' and benchmark='{}'".format(
                            table_target,
                            df_each['fund_id'].get_values()[0],
                            df_each['statistic_date'].get_values()[0],
                            df_each['benchmark'].get_values()[0])
                    print "sql_del:", sql_del
                    engine_target.execute(sql_del)
                    df_each.to_sql(table_target, engine_target, if_exists='append', index=False)


def main():
    sync_2g_to_4g_product_big()
    sync_2g_to_4g_product_small()

if __name__ == "__main__":
    main()
