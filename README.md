remark of demo
###weixinfaguolai.py
uid
###web_wiews.py

    from api.settings import DATABASES
    engine_rd = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(
        DATABASES['default']['USER'],
        DATABASES['default']['PASSWORD'],
        DATABASES['default']['HOST'],
        DATABASES['default']['PORT'],
        DATABASES['default']['NAME'],
    ), connect_args={"charset": "utf8"})

<br>    

    import json
    resp_data = {'dates_reverse': date_list, 'dates': date_list[::-1], 'data': data_list, 'data_interpolate': data_interpolate_list}
    resp_data = json.dumps(resp_data)

<br>

    from rest_framework import permissions, status
    from rest_framework.response import Response
    from rest_framework.decorators import api_view, permission_classes

<br>

    # 插值数据
            df_interpolate = origina_data.interpolate()  # 对空值进行线性插值
            df_interpolate = df_interpolate.drop('statistic_date', axis=1)
            print(df_interpolate.head())
            
<br>

    # 累计收益率计算
            df_interpolate['return_nav'] = df_interpolate['swanav']/df_interpolate['swanav'].tolist()[0] - 1
            if df_interpolate['return_nav'].isnull().all():
                df_interpolate['return_nav'] = df_interpolate['added_nav'] / df_interpolate['added_nav'].tolist()[0] - 1
            if df_interpolate['return_nav'].isnull().all():
                df_interpolate['return_nav'] = df_interpolate['nav'] / df_interpolate['nav'].tolist()[0] - 1
            print(df_interpolate['return_nav'].head())
            
###sync_guotai_db.py

    #多进程
    import multiprocessing
    pool = multiprocessing.Pool(processes=4)
    pool.map(_sync_big_helper, tbs)
    pool.close()
    pool.join()

<br>

    engine_target.execute(sql_del)#直接执行删除命令
    df_each.to_sql(table_target, engine_target,if_exists='append', index=False)#使用pandas的to_sql方法把数据放入数据库
    
###sync_4g_product.py

    #coulumns相交和相差的列表名（intersection,difference, limit1）
    cols1 = pd.read_sql('select * from base.{} limit 1'.format(table_source), engine).columns.tolist()
    cols2 = pd.read_sql('select * from base.{} limit 1'.format(table_target), engine).columns.tolist()
    cols4 = pd.read_sql('select * from base.{} limit 1'.format(table_sub), engine).columns.tolist()
    cols_r = list(set(cols2).intersection(set(cols1)))
    cols_s = list((set(cols4).intersection(set(cols2))).difference(set(cols_r)))
    
<br>
###replace_into_And_insert.py

    #mysql中常用的三种插入数据的语句:
    #insert into表示插入数据，数据库会检查主键（PrimaryKey），如果出现重复会报错；
    #replace into表示插入替换数据，需求表中有PrimaryKey，或者unique索引的话，如果数据库已经存在数据，则用新数据替换，如果没有数据效果则和insert into一样；
    #REPLACE语句会返回一个数，来指示受影响的行的数目。该数是被删除和被插入的行数的和。如果对于一个单行REPLACE该数为1，则一行被插入，同时没有行被删除。如果该数大于1，则在新行被插入前，有一个或多个旧行被删除。如果表包含多个唯一索引，并且新行复制了在不同的唯一索引中的不同旧行的值，则有可能是一个单一行替换了多个旧行。
    #insert ignore表示，如果中已经存在相同的记录，则忽略当前新数据；
    
###models1.py

    result.index = pd.DatetimeIndex(result.index)
    result = result.resample('M').last()
    
    df_corr = datas.rolling(window=12).corr(pairwise=True, other=pe_data).iloc[11:, :, 0].T
    
###私募云通指数/models.py

    #私募云通指数累计收益率
    此处为累计收益率，取月频指数，表格中展示指数的月度收益
    #累计收益
    def accumulate_return(self):
        indexs = self.data()
        result = indexs / indexs.iloc[0, :] - 1
        return result
    #指数相关性
    下拉框中为私募云通指数，bennchmark选择分为市场指数和云通指数，如下图，红框处控制左右两图
    左图：关注指数和benchmark指数相关性
    @staticmethod
    def dynamic_coefficient(index_id: str):
        pe = PeIndex(index_id, benchmarks=_benchmarks, date_range=year_now())
        datas = pe.data(total=True).iloc[-23:, :]
        pe_data = pd.DataFrame(datas.pop(index_id))
        df_corr = datas.rolling(window=12).corr(pairwise=True, other=pe_data).iloc[11:, :, 0].T
        return df_corr
    右图：策略相关性
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
    