# coding:utf-8

"""
product数据库同步
"""

import sys
import platform

if platform.system() == "Linux":
    sys.path.append("/mydata/code/sync_center")
sys.path.append('E:\Documents\Python Projects\sync_center')
import numpy as np
import pandas as pd
import multiprocessing
import datetime
from db_config import engine_2g_base, engine_4g_product
from guotai.utils.io import to_sql
from sqlalchemy import create_engine
from apscheduler.schedulers.blocking import BlockingScheduler


def sync_4g_product_small():
    tables_small_sets = [
        'fund_info',
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
        'fund_allocation_data',
        'fund_fee_data',
        'fund_portfolio',
        'manager_resume',
        'org_executive_info',
        'fund_nv_standard_w',
        'fund_nv_standard_m',
        'index_monthly_risk',
        'index_monthly_return',
        'index_monthly_subsidiary',
        'index_weekly_risk',
        'index_weekly_return',
        'index_weekly_subsidiary'
    ]
    pool = multiprocessing.Pool(processes=2)
    pool.map(_sync_product_small_helper, tables_small_sets)
    pool.close()
    pool.join()


def _sync_product_small_helper(table):
    """
    # 对于小数据集
    :return:
    """

    engine_source = engine_2g_base
    engine_target = engine_4g_product

    target_col = set(pd.read_sql('select * from {} limit 1'.format(table), engine_target).columns)
    source_col = set(pd.read_sql('select * from {} limit 1'.format(table), engine_source).columns)
    drop_list = list(source_col.difference(target_col))

    # 先清空表,再全部写入
    sql = "select count(*) as cnt from {}".format(table)
    df1 = pd.read_sql(sql, engine_source)
    df4 = pd.read_sql(sql, engine_target)
    count1 = df1['cnt'].get_values()[0]
    count4 = df4['cnt'].get_values()[0]
    print "{} 源表:{}条; 目标表:{}条".format(table, count1, count4)
    if count1 == 0:
        return

    # 清空目标表内容
    sql_del = "delete from {}".format(table)
    engine_target.execute(sql_del)

    # 全部写入
    step = 10000
    start = 0
    count_now = 0

    for i in range(start, count1, step):
        sql = "select * from {} limit {}, {}".format(table, i, step)
        if table == 'fund_type_mapping':
            sql = "select * from {} WHERE flag = 1 limit {}, {}".format(table, i, step)
        print sql
        df = pd.read_sql(sql, engine_source)
        if table == 'fund_type_mapping':
            df = df.sort_values(by=['update_time'], ascending=False).drop_duplicates(['fund_id', 'typestandard_code'])
            df = df[df['typestandard_code'] > 10]
        df = df.fillna(np.NaN)
        df = df.drop(drop_list, 1)
        count_now += len(df)
        print "{}: {}/{}".format(table, count_now, count1)
        df.to_sql(table, engine_target, if_exists='append', index=False)


def sync_4g_product_big():
    """
    # 对于大数据集
    :return:
    """

    tables_bigsets = [
        'fund_nv_data_source',
        'fund_nv_data_standard',
        'fund_weekly_return',
        'fund_weekly_risk',
        'fund_month_return',
        'fund_month_risk', ]
    pool = multiprocessing.Pool(processes=2)
    pool.map(_sync_big_helper, tables_bigsets)
    pool.close()
    pool.join()


def _sync_big_helper(table):
    print table
    engine_source = engine_2g_base
    engine_target = engine_4g_product

    # 增量更新
    table_source = table_target = table
    if table_source in ['fund_month_risk', 'fund_weekly_risk']:
        table_source += '_tmp'
    check_keys(table_target, engine_source=engine_source)
    target_col = set(pd.read_sql('select * from {} limit 1'.format(table_target), engine_target).columns)
    source_col = set(pd.read_sql('select * from {} limit 1'.format(table_source), engine_source).columns)
    drop_list = list(source_col.difference(target_col))

    sql_mut = "select fund_id , max(update_time) as mut from {} GROUP BY fund_id"
    df_mut_target = pd.read_sql(sql_mut.format(table_target), engine_target)
    df_mut_source = pd.read_sql(sql_mut.format(table_source), engine_source)
    df_mut = compare_df(df_mut_source, df_mut_target)
    print('{}:{}'.format(table_target, len(df_mut)))
    # 按基金id分别读取
    for i in range(len(df_mut)):
        fund_id = df_mut['fund_id'].get_values()[i]
        max_update_time = df_mut['mut'].get_values()[i]
        max_update_time = np.datetime64(max_update_time, 's').astype(datetime.datetime)
        sql = "select * from {} where fund_id='{}' and update_time > '{}'".format(table_source, fund_id,
                                                                                  str(max_update_time))
        print(sql)
        df = pd.read_sql(sql, engine_source)
        df = df.drop(drop_list, 1)
        df = df.fillna(np.NaN)
        print(table_target, fund_id, len(df))
        to_sql(table_target, engine_target, df)
        print('{} {}: {} Done!'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), table_target, fund_id))


"""

        target_col = set(pd.read_sql('select * from {} limit 1'.format(table_target), engine_target).columns)
    cols = pd.read_sql('select * from {} limit 1'.format(table_source), engine_source).columns
    source_col = set(cols)
    drop_list = list(source_col.difference(target_col))

    target_mut = str(
        pd.read_sql("select max(update_time) as mut from {}".format(table_target), engine_target)['mut'].get_value(0))
    print target_mut
    datas = engine_source.execute("select * from {} WHERE update_time > '{}'".format(table_source, target_mut))
    num = pd.read_sql("select count(*) AS nm from {} WHERE update_time > '{}'".format(table_source, target_mut),
                      engine_source)['nm'].get_value(0)
    print "{}: {} New Data!".format(table_target, num)
    num2 = 0
    while True:
        data = datas.fetchmany(1000)
        if len(data) == 0:
            break
        df = pd.DataFrame(data)
        df.columns = cols
        df = df.drop(drop_list, 1)
        num2 += 1000
        to_sql(table_target, engine_target, df)
        print "{}: {}/{}".format(table_target, num2, num)
"""


def check_keys(table, engine_source=engine_2g_base, engine_target=engine_4g_product):
    """
    对比主键，同步删除已在base库中被删除的数据
    :return:
    """
    table_target = table_source = table
    id_list = pd.read_sql("select distinct fund_id from {}".format(table_target),
                          engine_target)['fund_id'].tolist()
    id_sets = get_id_sets(id_list)
    sd_key = "statistic_date"
    keys = ['fund_id', 'sd']
    if table in ["fund_nv_standard_w", "fund_nv_standard_m"]:
        sd_key += "_std"
    for ids in id_sets:
        sql_key = 'SELECT DISTINCT fund_id,{} AS sd from {} WHERE fund_id IN {}'
        if table == "fund_nv_data_source":
            sql_key = 'SELECT DISTINCT fund_id,{} AS sd,data_source from {} WHERE fund_id IN {}'
            keys = ['fund_id', 'sd', 'data_source']
        df_target = pd.read_sql(sql_key.format(sd_key, table_target, ids), engine_target)
        df_source = pd.read_sql(sql_key.format(sd_key, table_source, ids), engine_source)
        df_target['t_ind'] = 1
        df_source['s_ind'] = 2
        df_uni = df_target.merge(df_source, how='outer', on=keys)
        df_uni['s_ind'] = df_uni['s_ind'].fillna(-100)
        df_del = df_uni.loc[df_uni['s_ind'] == -100][keys]
        df_del.index = range(len(df_del))
        del_idset = set(df_del['fund_id'].tolist())
        if table == "fund_nv_data_source":
            for i in range(len(df_del)):
                del_id = df_del['fund_id'][i]
                del_date = df_del['sd'][i]
                del_source = df_del['data_source'][i]
                print(del_id, del_date, del_source)
                sql_del = "delete from fund_nv_data_source WHERE  fund_id = '{}' AND statistic_date = '{}' AND data_source = {}".format(
                    del_id, del_date, del_source)
                print(sql_del)
                engine_target.execute(sql_del)
        else:
            for del_id in del_idset:
                del_sd_set = []
                for sd in df_del.loc[df_del['fund_id'] == del_id]['sd'].get_values():
                    del_sd_set.append("'{}'".format(sd.strftime("%Y-%m-%d")))
                del_sd_set = ','.join(del_sd_set)
                sql_del = "delete from {} WHERE  fund_id = '{}' AND {} IN ({})".format(table_target, del_id, sd_key,
                                                                                       del_sd_set)
                engine_target.execute(sql_del)


def compare_df(df1, df2):
    """
    # 返回需要更新的fund_id,max(update_time)
    """
    df = df1.merge(df2, how='left', suffixes=('_1', '_2'), on=['fund_id'])
    df.mut_2 = df.mut_2.fillna(pd.tslib.Timestamp(1970, 1, 1))
    df_mut = df[df.mut_1 > df.mut_2][['fund_id', 'mut_2']]
    df_mut.columns = ['fund_id', 'mut']
    return df_mut


def sync_risk_local(table_dict):
    engine = create_engine("mysql+pymysql://sm01:y63TAftg@58cb57c164977.sh.cdb.myqcloud.com:4171",
                           connect_args={"charset": "utf8"}, pool_size=8)

    table = table_dict.split(':')[0]
    table_sub = table_dict.split(':')[1]
    table_source = table_target = table
    if table in ['fund_weekly_risk', 'fund_month_risk']:
        table_target += '_tmp'
    engine.execute("delete from base.{}".format(table_target))
    print("{} Delete Old Data in {}!".format(datetime.datetime.now().strftime("%H:%M:%S"), table_target))
    cols1 = pd.read_sql('select * from base.{} limit 1'.format(table_source), engine).columns.tolist()
    cols2 = pd.read_sql('select * from base.{} limit 1'.format(table_target), engine).columns.tolist()
    cols4 = pd.read_sql('select * from base.{} limit 1'.format(table_sub), engine).columns.tolist()
    cols_r = list(set(cols2).intersection(set(cols1)))
    cols_s = list((set(cols4).intersection(set(cols2))).difference(set(cols_r)))
    cols = cols_r + cols_s
    cols_insert = list_to_str(cols)
    cols_base = list_to_str(cols_r, 'fwr.') + ',' + list_to_str(cols_s, 'fswi.')
    id_list = pd.read_sql("select distinct fund_id from base.{}".format(table), engine)['fund_id'].tolist()
    for i in range(0, len(id_list), 1000):
        sql_update_all = "insert into base.{} ({})select {} from base.{} fwr JOIN base.{} fswi ON fwr.fund_id = fswi.fund_id AND fwr.statistic_date = fswi.statistic_date AND fwr.benchmark = fswi.benchmark WHERE  fwr.fund_id IN ({})".format(
            table_target, cols_insert, cols_base, table_source, table_sub,
            id_to_str(id_list[i:min([i + 1000, len(id_list)])]))
        print("{} Start to Update {}!".format(datetime.datetime.now().strftime("%H:%M:%S"), table_target))
        engine.execute(sql_update_all)
        print("{} {}/{} All Updated!".format(datetime.datetime.now().strftime("%H:%M:%S"),
                                             min([i + 1000, len(id_list)]), table_target))


def list_to_str(li, sub=''):
    cols3 = []
    for c in li:
        cols3.append(sub + str(c))
    cols = ','.join(cols3)
    return cols


def id_to_str(li, sub=''):
    cols3 = []
    for c in li:
        cols3.append(sub + "'{}'".format(c))
    cols = ','.join(cols3)
    return cols


def sync_risk():
    table_list = ['fund_weekly_risk:fund_subsidiary_weekly_index', 'fund_month_risk:fund_subsidiary_month_index']
    for table_dict in table_list:
        sync_risk_local(table_dict)


def get_id_sets(df_ids):
    """
    拼接字符
    :return:
    """
    count_num = 0
    id_sets = []
    ids_set_part = "("
    for ids in df_ids:
        ids_set_part += "'{}',".format(ids)
        count_num += 1
        if count_num % 100 == 0 or count_num == len(df_ids):
            ids_set_part = ids_set_part[0:-1]
            ids_set_part += ")"
            id_sets.append(ids_set_part)
            ids_set_part = "("
    return id_sets


def sync_product_small():
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sync_4g_product_small()
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def sync_product_big():
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sync_risk()
    sync_4g_product_big()
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def main():

    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sync_4g_product_small()
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sync_risk()
    sync_4g_product_big()
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    sched = BlockingScheduler()
    sched.add_job(sync_product_small, 'cron', hour='5', minute='30')  # 6:00,
    sched.add_job(sync_product_big, 'cron', hour='8', minute='30')
    sched.start()
    # main()
