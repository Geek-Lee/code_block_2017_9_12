#!/usr/bin/env python
# encoding: utf-8


from rest_framework import permissions, status
from rest_framework.response import Response
from rest_framework.decorators import api_view, permission_classes
from sqlalchemy import create_engine
from ht_competition.settings import DATABASES
import pandas as pd
from utils import helpfunc, io
import datetime as dt
from collections import OrderedDict


engine_ht = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(
    DATABASES['default']['USER'],
    DATABASES['default']['PASSWORD'],
    DATABASES['default']['HOST'],
    DATABASES['default']['PORT'],
    DATABASES['default']['NAME'],
), connect_args={"charset": "utf8"})

_org_info = [
    'org_name', 'org_full_name', 'reg_code', 'reg_time', 'found_date', 'reg_capital', 'real_capital', 'region',
    'profile',
    'address', 'team', 'fund_num', 'is_qualification', 'prize', 'team_scale', 'investment_idea', 'master_strategy',
    'asset_mgt_scale', 'linkman', 'linkman_duty', 'linkman_phone', 'linkman_email']

_manager_info = ['user_name', 'sex', 'org_name',
                 'introduction', 'entry_date', 'investment_years', 'education',
                 'duty', 'qualification', 'background', 'is_fund_qualification',
                 'is_core_member', 'resume', 'max_asset_mgt_scale', 'prize']

_fund_info = ['match_group', 'fund_type_strategy', 'reg_code', 'foundation_date',
              'fund_name', 'fund_full_name', 'fund_manager', 'fund_manager_nominal', 'fund_stockbroker',
              'fund_custodian', 'fund_member', 'fund_type_issuance', 'fund_type_structure', 'fund_structure',
              'issue_scale', 'asset_scale', 'is_main_fund', 'fee_pay', 'open_date', 'locked_time_limit',
              'duration', 'fee_manage', 'fee_pay_remark', 'fee_redeem', 'fee_subscription',
              'fee_trust', 'investment_range', 'min_purchase_amount', 'min_append_amount',
              'stop_line', 'alert_line', 'manager_participation_scale', 'investment_idea']

_fund_nv = []

_digital = OrderedDict(
    [('万亿', 10 ** 12),
     ('千亿', 10 ** 11),
     ('百亿', 10 ** 10),
     ('十亿', 10 ** 9),
     ('亿', 10 ** 8),
     ('千万', 10 ** 7),
     ('百万', 10 ** 6),
     ('十万', 10 ** 5),
     ('万', 10 ** 4),
     ('千', 10 ** 3),
     ('百', 10 ** 2),
     ('十', 10 ** 1),
     ('元', 10 ** 0)])

#前两个api_view是上传excel的页面
@api_view(http_method_names=['POST', 'GET'])
@permission_classes((permissions.AllowAny,))
def parser_nav_file(request):
    uid = request.data.get('uid')
    filefullpath = request.data.get('filefullpath')
    fund_id = request.data.get('fund_id')
    result = handle_uploaded_nav(filefullpath, uid, fund_id)
    return Response({'nav': result}, status=status.HTTP_200_OK)
#从request取得uid，filefullpath，fund_id，传入函数处理返回参数result

@api_view(http_method_names=['POST', 'GET'])
@permission_classes((permissions.AllowAny,))
def parser_fund_file(request):
    uid = request.data.get('uid')
    filefullpath = request.data.get('filefullpath')
    org_status = handle_uploaded_org(filefullpath, uid)
    manager_status = handle_uploaded_manager(filefullpath, uid)
    fund_status = handle_uploaded_fund(filefullpath, uid)
    nav_status = handle_uploaded_nav(filefullpath, uid)
    result = {'nav': nav_status, 'fund': fund_status, 'org': org_status, 'manager': manager_status}
    return Response(result, status=status.HTTP_200_OK)

###########################################################################################

@api_view(http_method_names=['POST', 'GET'])
@permission_classes((permissions.AllowAny,))
def investment_company(request):
    """
    投顾公司操作：
    Path+investment_company/
    一个用户只能创建一个投顾公司，添加后不可删除

    A.	添加或更新：
    Params = {'method': 'update', 'uid': uid}
    B.	查询：
    Params = {'method': 'query', 'uid': uid}

    :param request:
    :return:
    """
    method = request.data.get('method')
    uid = request.data.get('uid')
    org_id = helpfunc.md5encoder(uid).upper()
    if method == 'query':
        cols = helpfunc.format_sql_columns(_org_info, 'tb_oi')
        sql = "SELECT DISTINCT tb_oi.org_id, {} FROM org_fund_manager_mapping tb_rfm " \
              "JOIN org_info tb_oi ON tb_oi.org_id = tb_rfm.org_id " \
              "WHERE tb_rfm.uid='{}'".format(cols, uid)
        org_data = pd.read_sql(sql, engine_ht)
        if len(org_data) != 0:
            org_data.fillna('', inplace=True)
            resp = {'succeed': True, 'org_info': org_data.to_dict(orient='index').get(0), 'org_id': org_id}
            #>>> a = {0: {'class': 11, 'name': 1, 'price': 111}, 1: {'class': 22, 'name': 2, 'price': 222},
            #          2: {'class': 33, 'name': 3, 'price': 333}}
            #>>> a[0]
            #{'class': 11, 'name': 1, 'price': 111}
        else:
            resp = {'succeed': False}
        return Response(resp, status=status.HTTP_200_OK)
    elif method == 'update':
        org_data = request.data.get('org_data')
        org_data['org_id'] = org_id
        org_data = pd.DataFrame([org_data])
        try:
            io.to_sql('org_info', engine_ht, org_data)
            return Response({'succeed': True, 'msg': '投顾信息添加成功'}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'succeed': False, 'error_info': str(e)}, status=status.HTTP_200_OK)


@api_view(http_method_names=['POST', 'GET'])
@permission_classes((permissions.AllowAny,))
def fund_manager(request):
    """
    基金经理操作：

    Path + fund_manager/

    A．添加基金经理：
    Params = {‘method’:’update’, ‘uid’:uid, ‘manager_data’: 表单数据}

    B. 更新基金经理信息：
    Params = {‘method’:’update’, ‘uid’:uid, ‘manager_data’: 表单数据, ‘manager_id’:manager_id}

    C. 按manager_id查询基金经理：
    Params = {‘method’:’query’, ‘uid’:uid, ‘manager_id’:可选 }

    D．查询某只特定基金的基金经理：
    Params = {‘method’: ‘query’, ‘uid’:uid, ‘fund_id’:fund_id,‘manager_id’:可选}

    E.删除基金经理：
    Params = {‘method’:’delete’, ‘uid’:’uid’, ‘manager_id’:manager_id}

    :param request:
    :return:
    """
    method = request.data.get('method')
    uid = request.data.get('uid')

    # 查询用户创建的所有基金经理，传入fund_id，则只给出该产品的基金经理的信息
    if method == 'query':
        fund_id = request.data.get('fund_id', None)
        cols = helpfunc.format_sql_columns(_manager_info, 'tb_mi')
        sql = "SELECT DISTINCT tb_mi.user_id, {} FROM org_fund_manager_mapping tb_rfm " \
              "JOIN manager_info tb_mi ON tb_mi.user_id = tb_rfm.user_id " \
              "WHERE tb_rfm.uid='{}'".format(cols, uid)
        manager_data = pd.read_sql(sql, engine_ht)
        manager_data.set_index('user_id', inplace=True)
        if len(manager_data) != 0:
            manager_ids = manager_data['user_name'].to_dict()
            #df = pd.DataFrame({'name': [1, 2, 3], "class": [11, 22, 33], "price": [111, 222, 333]})
            #>>> df['class'].to_dict()
            #{0: 11, 1: 22, 2: 33}
            if fund_id:
                manager_ids = fund_manager_mapping(fund_id)
            manager_id = request.data.get('manager_id', list(manager_ids.keys())[0])
            #manager_id = request.data.get('manager_id', manager_id)
            manager_info = manager_data.loc[manager_id].fillna('').to_dict()
            #定位index为manager_id的那一行，空的填充为''，变成字典
            #>>> df.loc[0].fillna('').to_dict()
            # {'class': '', 'name': 1.0, 'price': 111.0}
            # >>>
            manager_info['manager_id'] = manager_id
            #把manager_info中的manager_id传入新值
            resp = {'manager_ids': helpfunc.to_list(manager_ids), 'manager_info': manager_info, 'succeed': True}
        else:
            resp = {'succeed': False}
        return Response(resp, status=status.HTTP_200_OK)

    # 更新或添加基金经理，已是否传入具体的manager_id区分
    elif method == 'update':
        manager_data = request.data.get('manager_data')
        manager_id = request.data.get('manager_id', None)
        manager_ids = query_info(uid, 'manager')
        #根据uid和type为manager，返回的是{user_id:user_name, user_id:user_name, user_id:user_name}
        ids_list = helpfunc.to_list(manager_ids)
        #返回的是[[user_id,user_name],[user_id,user_name],[user_id,user_name],[user_id,user_name]]
        msg = '基金经理信息更新成功'
        if manager_data['user_name'] not in manager_ids.values() and not manager_id:
            #如果manager_data中的user_name不在数据库，且manger_id是空的，说明是新的一天记录
            #在ids_list中添加manager_data的user_id和user_name信息
            manager_data['user_id'] = helpfunc.md5encoder(uid + manager_data['user_name']).upper()
            ids_list.append([manager_data['user_id'], manager_data['user_name']])
            msg = '基金经理信息添加成功'
        df = pd.DataFrame([manager_data])
        try:
            io.to_sql('manager_info', engine_ht, df)
            return Response({'succeed': True, 'manager_ids': ids_list, 'msg': msg}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'succeed': False, 'manager_ids': ids_list, 'error_info': str(e), 'msg': '操作失败'},
                            status=status.HTTP_200_OK)

    # 删除基金经理
    elif method == 'delete':
        try:
            manager_id = request.data.get('manager_id')
            msg = delete_manager(manager_id, uid)
            return Response({'succeed': True, 'msg': msg}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'succeed': False, 'error_info': str(e), 'msg': '基金经理删除失败'}, status=status.HTTP_200_OK)


@api_view(http_method_names=['POST', 'GET'])
@permission_classes((permissions.AllowAny,))
def funds(request):
    """
    基金产品操作：
    Path + fund/

    A.	添加产品：
    Params = {‘method’:’update’, ‘uid’:uid, ‘fund_data’: 表单数据}

    B.	更新产品信息：
    Params = {‘method’:’update’, ‘uid’:uid, ‘fund_data’: 表单数据}

    C．按fund_id查询产品信息：
    Params = {‘method’:’query’, ‘uid’:uid, ‘fund_id’:可选 }

    D．删除产品：
    Params = {‘method’:’delete’, ‘uid’:uid, ‘fund_id’:manager_id}

    :param request:
    :return:
    """
    method = request.data.get('method')
    uid = request.data.get('uid')

    # 查询产品信息
    if method == 'query':
        cols = helpfunc.format_sql_columns(_fund_info, 'tb_fi')
        sql = "SELECT DISTINCT tb_fi.fund_id,{} FROM org_fund_manager_mapping tb_rfm " \
              "JOIN fund_info tb_fi ON tb_fi.fund_id = tb_rfm.fund_id " \
              "WHERE tb_rfm.uid='{}'".format(cols, uid)
        fund_data = pd.read_sql(sql, engine_ht)
        fund_data.set_index('fund_id', inplace=True)
        if len(fund_data) != 0:
            fund_ids = fund_data['fund_name'].to_dict()
            fund_id = request.data.get('fund_id', list(fund_ids.keys())[0])
            #以上几步都是通过uid查询fund_id
            #下面这几步把df变成字典，因为fund_id变成index用于易于查询，所以还需再插入fund_id到字典中
            fund_info = fund_data.loc[fund_id].fillna('').to_dict()
            fund_info['fund_id'] = fund_id
            #{'class': '', 'name': 1.0, 'price': 111.0, 'fund_id': 100}
            resp = {'fund_ids': helpfunc.to_list(fund_ids), 'fund_info': fund_info, 'succeed': True}
        else:
            resp = {'succeed': False}
        return Response(resp, status=status.HTTP_200_OK)

    # 更新或删除产品信息， 已是否传入具体的fund_id区分
    elif method == 'update':
        fund_data = request.data.get('fund_data')
        fund_id = request.data.get('fund_id', None)
        fund_ids = query_info(uid, 'fund')
        ids_list = helpfunc.to_list(fund_ids)
        msg = '产品信息更新成功'

        #如果fund_name不在数据库，并且fund_id没有传入，添加id和name
        if fund_data['fund_name'] not in fund_ids.values() and not fund_id:
            fund_data['fund_id'] = helpfunc.md5encoder(uid + fund_data['fund_name']).upper()
            ids_list.append([fund_data['fund_id'], fund_data['fund_name']])
            msg = '产品信息添加成功'
        df = pd.DataFrame([fund_data])
        try:
            io.to_sql('fund_info', engine_ht, df)
            msg = update_fund(df, uid, msg, refresh=True)
            #msg信息中增加了一些消息
            return Response({'succeed': True, 'fund_ids': ids_list, 'msg': msg}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'succeed': False, 'fund_ids': ids_list, 'error_info': str(e), 'msg': '操作失败'},
                            status=status.HTTP_200_OK)

    # 删除产品
    elif method == 'delete':
        try:
            fund_id = request.data.get('fund_id')
            sql_delete = "DELETE FROM fund_index WHERE fund_id = '{fid}'; " \
                         "DELETE FROM fund_info WHERE fund_id = '{fid}'; " \
                         "DELETE FROM fund_nav_data WHERE fund_id = '{fid}'; " \
                         "DELETE FROM org_fund_manager_mapping WHERE fund_id = '{fid}';".format(fid=fund_id)
            engine_ht.execute(sql_delete)
            return Response({'succeed': True, 'msg': '产品信息删除成功'}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'succeed': False, 'error_info': str(e), 'msg': '产品信息删除失败'}, status=status.HTTP_200_OK)


@api_view(http_method_names=['POST', 'GET'])
@permission_classes((permissions.AllowAny,))
def fund_static(request):
    """
    数据填报主页，查询用户上传的基金详情
    :param uid: 用户uid
    :return:
    """
    uid = request.data.get('uid')
    sql = "SELECT tb_ofm.fund_id, tb_fi.fund_name, tb_oi.org_name, tb_mi.user_name, tb_ofm.`match_group`, " \
          "tb_fi.fund_type_strategy, tb_fi.issue_scale, DATE_FORMAT(tb_fnd.statistic_date, '%%Y-%%m-%%d') AS nav_date, " \
          "tb_fnd.nav, tb_fnd.added_nav FROM org_fund_manager_mapping tb_ofm " \
          "JOIN fund_info tb_fi ON tb_fi.fund_id = tb_ofm.fund_id " \
          "JOIN org_info tb_oi ON tb_oi.org_id = tb_ofm.org_id " \
          "JOIN manager_info tb_mi ON tb_ofm.user_id = tb_mi.user_id " \
          "JOIN fund_nav_data tb_fnd ON tb_fnd.fund_id = tb_ofm.fund_id AND tb_fnd.statistic_date IN " \
          "(SELECT max(statistic_date) FROM fund_nav_data WHERE fund_id = tb_ofm.fund_id) ORDER BY nav_date DESC " \
          "WHERE tb_ofm.uid='{}'".format(uid)
    #从五个表中提取以上信息，最后一个条件是id=id且时间符合方位，排序方式为降序
    fund_static_data = pd.read_sql(sql, engine_ht)
    if len(fund_static_data):
        #加入基金经理姓名为一组，去重，加index，填充
        for fund_id in fund_static_data['fund_id'].unique():
            managers = fund_static_data.loc[fund_static_data['fund_id'] == fund_id]['user_name'].tolist()
            managers = ','.join(managers)
            fund_static_data.loc[fund_static_data['fund_id'] == fund_id, ['user_name']] = managers
        fund_static_data.drop_duplicates()
        fund_static_data.index = range(1, len(fund_static_data)+1)
        fund_static_data.fillna('-', inplace=True)
        resp = {'fund_static': helpfunc.trans_data_to_dict(fund_static_data, use_for='table',
                                                           rename_columns=['fund_id', '基金简称', '投资顾问', '基金经理', '参数组别',
                                                                           '投资策略', '发行规模', '最新净值日期', '单位净值', '累计净值'],
                                                           index_name='序号'),
                'succeed': True}
        #最后一个把dataframe变成dict以供前端使用
    else:
        resp = {'succeed': False}
    return Response(resp, status=status.HTTP_200_OK)


def update_fund(df_fund_info, uid, msg, refresh=False):
    fund_info = df_fund_info.to_dict(orient='record')[0]
    #[{'class': 11, 'name': 1, 'price': 111}, {'class': 22, 'name': 2, 'price': 222}, {'class': 33, 'name': 3, 'price': 333}]
    #{'class': 11, 'name': 1, 'price': 111}
    manager_map = helpfunc.reverse_dict(query_info(uid, 'manager'))
    #{id: name, id:name, id:name},并对其进行翻转

    new_manager = []
    manager_id = list(map(lambda y: manager_map.get(y, y), fund_info['fund_member'].split(',')))
    #>>> mi = list(map(lambda x: mm.get(x,x), ['name','price']))
    #>>> mi
    #[1, 111]
    #dict.get(key[, default])
    #当取不到值时，如果给出默认值，返回default值
    #遍历manager_id的值，如果不在manager_map，去除manager_id中的id，在列表中增加id
    for mid in manager_id:
        if mid not in manager_map.values():
            manager_id.remove(mid)
            new_manager.append(mid)
            manager_id.append(helpfunc.md5encoder(uid+mid).upper())

    if len(new_manager) != 0:
        msg += '，请补充基金经理{}的信息'.format(','.join(new_manager))


    #生成一个新的df，放入数剧表org_fund_manager_mapping
    org_id = helpfunc.md5encoder(uid).upper()
    fund_id = fund_info['fund_id']
    group = fund_info['match_group']
    new_record = pd.DataFrame([[org_id], [fund_id], [group], [uid], manager_id],
                              index=['org_id', 'fund_id', 'match_group', 'uid', 'user_id']).T.fillna(method='ffill')
    io.to_sql('org_fund_manager_mapping', engine_ht, new_record)

    if refresh:
        dict_fund_name = df_fund_info[['fund_id', 'fund_name']].to_dict(orient='record')
        for di in dict_fund_name:
            sql_refresh = "UPDATE fund_index SET fund_name = '{fn}' WHERE fund_id = '{fid}';" \
                          "UPDATE fund_nav_data SET fund_name = '{fn}' WHERE fund_id = '{fid}';".format(fn=di['fund_name'],fid=di['fund_id'])
            try:
                engine_ht.execute(sql_refresh)
            except:
                pass
    return msg


def delete_manager(manager_id, uid):
    #查取user_id,fund_id从org_fund_manager_mapping，条件为user_id为manager_id
    sql = "SELECT DISTINCT user_id,fund_id FROM org_fund_manager_mapping WHERE user_id = '{}'".format(manager_id)
    result = pd.read_sql(sql, engine_ht)
    manager_name = query_info(uid, 'manager').get(manager_id)
    #根据uid查出{manager_id:manager_name}的键值对，通过get取出manager_name
    if len(result):
        fund_ids = result['fund_id'].tolist()
        fund_name_dict = query_info(uid, 'fund')
        relate_fund = [fund_name_dict.get(fid) for fid in fund_ids]
        #遍历从map中提取的fund_id列表，通过fund_id提取在fund_info表中的fund_name
        msg = '基金经理{}已与产品{}关联，请修改产品的基金经理信息后再进行删除操作'.format(manager_name, ','.join(relate_fund))
    else:
        sql_del = "DELETE FROM manager_info WHERE user_id = '{mi}'; " \
                  "DELETE FROM org_fund_manager_mapping WHERE user_id = '{mi}'; ".format(mi=manager_id)
        msg = '基金经理信息删除成功'
        engine_ht.execute(sql_del)
    return msg


def query_info(uid, types):
    _type_mapping = {'fund': 'fund', 'manager': 'user', 'org': 'org'}
    ids = _type_mapping.get(types)
    # >>> ids = _type_mapping.get('manager')
    # >>> ids
    # 'user'
    sql = "SELECT DISTINCT tb_i.{k}_id,tb_i.{k}_name FROM org_fund_manager_mapping tb_rfm " \
          "JOIN {tb}_info tb_i ON tb_i.{k}_id = tb_rfm.{k}_id " \
          "WHERE tb_rfm.uid='{uid}'".format(uid=uid, k=ids, tb=types)
    #查找{k}_id,{k}_name从tb_frm,{tb}_info,条件是id相等且tb_rfm.uid='{uid}'
    result = pd.read_sql(sql, engine_ht).fillna('')
    #执行sql语句，并对其进行fillna('')
    result = result.set_index('{}_id'.format(ids))['{}_name'.format(ids)].to_dict()
    #把id变成index，把name导出来，形式为{id: name, id:name, id:name}或{id:name}
    return result


def fund_manager_mapping(fund_id):
    # 从数据库提取fun_id为XX的user_id，并且变成list
    sql = "SELECT user_id FROM org_fund_manager_mapping WHERE fund_id = '{}'".format(fund_id)
    manager_ids = pd.read_sql(sql, engine_ht)['user_id'].tolist()
    #从数据库提取user_id为在manager_ids的列表中的数据，提取user_id，user_name
    #把user_id变成index，提取user_name变成字典
    sql_manager = "SELECT user_id, user_name FROM manager_info " \
                  "WHERE user_id IN ({})".format(helpfunc.format_sql_value(manager_ids))
    managers = pd.read_sql(sql_manager, engine_ht).set_index('user_id')['user_name'].to_dict()
    return managers


def handle_uploaded_nav(filefullpath, uid, fund_id=None, sheet='基金净值'):
    _cols = ['fund_name', 'statistic_date', 'nav', 'added_nav', 'total_share', 'total_asset', 'total_nav', 'is_split',
             'is_open_date', 'split_ratio', 'after_tax_bonus']
    # _bool_dict = {'是': 1, '否': '2'}
    try:
        df_nav = pd.read_excel(filefullpath, sheetname=sheet).dropna(how='all').fillna(method='ffill')
        df_nav.columns = _cols
        fund_id_mapping = query_info(uid, 'fund')# {fund_id: fund_name}
        # 如果存在fund_id则传给id，否则根据名字生成fund_id
        if fund_id:
            df_nav['fund_id'] = fund_id
            df_nav['fund_name'] = fund_id_mapping.get(fund_id)
        else:
            df_nav['fund_id'] = df_nav['fund_name'].apply(
                lambda x: helpfunc.reverse_dict(fund_id_mapping).get(x, helpfunc.md5encoder(uid + x).upper()))
        df_nav.loc[:, ['statistic_date']] = df_nav['statistic_date'].apply(date_parser)
        # df_nav.loc[: ['is_split', 'is_open_date']] = df_nav[['is_split', 'is_open_date']].applymap(lambda x: _bool_dict.get(x))
        io.to_sql('fund_nav_data', engine_ht, df_nav)
        result = {'succeed': True, 'msg': '净值数据解析成功'}
    except Exception as e:
        result = {'succeed': False, 'error_info': str(e), 'msg': '净值数据解析失败'}
    return result


def handle_uploaded_org(filefullpath, uid, sheet='公司简介'):

    _cols = ["org_name", "org_full_name", "reg_code", "reg_time", "found_date", "reg_capital",
             "real_capital", "region", "profile", "address", "team", "fund_num",
             "is_qualification", "prize", "team_scale", "investment_idea", "master_strategy",
             "remark", "asset_mgt_scale", "linkman", "linkman_duty", "linkman_phone", "linkman_email"]
    try:
        df_org = pd.read_excel(filefullpath, sheetname=sheet, index_col=0).T.dropna(how='all')
        df_org.columns = _cols
        df_org.dropna(subset=['org_name', 'org_full_name'], how='all', inplace=True)
        if len(df_org) == 0:
            error_info = '上传的投顾公司简介必须包含公司名称'
            raise AssertionError(error_info)
        df_org.index = range(len(df_org))
        #df_org中的org_name是空的，传入org_full_name
        df_org.loc[df_org['org_name'].isnull(), ['org_name']] = df_org.loc[df_org['org_name'].isnull()]['org_full_name']

        df_org['org_id'] = df_org['org_name'].apply(lambda x: helpfunc.md5encoder(uid).upper())
        #如果有x进行运算，否则x直接为‘-’
        df_org.loc[:, ['is_qualification']] = df_org['is_qualification'].apply(lambda x: x.split('-')[-1] if x else '-')

        df_org.loc[:, ['reg_time', 'found_date']] = df_org[['reg_time', 'found_date']].applymap(
            lambda x: date_parser(x) if x else None)
        df_org.loc[:, ["reg_capital", "real_capital", "asset_mgt_scale"]] = df_org[
            ["reg_capital", "real_capital", "asset_mgt_scale"]].applymap(
            lambda x: money_string(x, unit='万') if x else None)

        io.to_sql('org_info', engine_ht, df_org)
        msg = '投顾公司信息上传成功'

        #查询公司名字不重合的部分，用join方法format进msg
        names = set(df_org['org_name'])
        #names为所有去重后的公司名字
        names_used = set(df_org.dropna(subset=["org_name", "org_full_name", "reg_code", "team", "fund_num",
                                               "is_qualification", "team_scale", "linkman", "linkman_phone"
                                               ], how='any')['org_name'])
        #names_used为信息为全的公司名字
        not_used = list(names.difference(names_used))
        #names-names_used的名字列表
        if len(not_used):
            msg += "请及时补充投顾公司:{}的关键信息".format(','.join(not_used))
        #notused = ['3', '4']
        #msg = ''
        #msg += "请及时补充投顾公司:{}的关键信息".format(','.join(notused))
        #msg
        #'请及时补充投顾公司:3,4的关键信息'
        result = {'succeed': True, 'msg': msg}
    except AssertionError as e:
        result = {'succeed': False, 'msg': str(e)}
    except Exception as e:
        result = {'succeed': False, 'msg': '投顾公司信息上传失败', 'error_info': str(e)}
    return result


def handle_uploaded_manager(filefullpath, uid, sheet='基金经理简介'):
    _cols = ["user_name", "sex", "org_name", "introduction", "photo", "entry_date",
             "investment_years", "education", "duty", "qualification", "background", "is_fund_qualification",
             "is_core_member", "resume", "max_asset_mgt_scale", "prize", "remark"]
    try:
        df_manager = pd.read_excel(filefullpath, sheetname=sheet, index_col=0).T.dropna(how='all')
        df_manager.columns = _cols
        df_manager.dropna(subset=['user_name'], inplace=True)
        if len(df_manager) == 0:
            raise AssertionError('上传的基金经理信息必须包含经理姓名')

        df_manager.index = range(len(df_manager))

        df_manager['user_id'] = df_manager['user_name'].apply(lambda x: helpfunc.md5encoder(uid+x).upper())
        df_manager.loc[:, ["is_fund_qualification", "is_core_member"]] = df_manager[
            ["is_fund_qualification", "is_core_member"]].applymap(lambda x: x.split('-')[-1])

        df_manager.loc[:, ["entry_date"]] = df_manager["entry_date"].apply(lambda x: date_parser(x) if x else None)
        df_manager.loc[:, ['max_asset_mgt_scale']] = df_manager['max_asset_mgt_scale'].apply(
            lambda x: money_string(x, unit='万') if x else None)
        io.to_sql('manager_info', engine_ht, df_manager)
        msg = '基金经理信息上传成功'

        names = set(df_manager['user_name'])
        names_used = set(df_manager.dropna(
            subset=["user_name", "introduction", "entry_date", "is_fund_qualification", "is_core_member"], how='any')[
                             'user_name'])
        not_used = list(names.difference(names_used))
        if len(not_used):
            msg += "请及时补充基金经理:{}的关键信息".format(','.join(not_used))
        result = {'succeed': True, 'msg': msg}
    except AssertionError as e:
        result = {'succeed': False, 'msg': str(e)}
    except Exception as e:
        result = {'succeed': False, 'msg': '基金经理信息解析失败', 'error_info': str(e)}
    return result


def handle_uploaded_fund(filefullpath, uid, sheet='基金简介'):
    _cols = ["match_group", "fund_type_strategy", "reg_code", "foundation_date", "fund_name",
             "fund_full_name", "fund_manager", "fund_manager_nominal", "fund_stockbroker",
             "fund_custodian", "fund_member", "fund_type_issuance", "fund_type_structure",
             "fund_structure", "issue_scale", "asset_scale", "is_main_fund", "fee_pay",
             "open_date", "locked_time_limit", "duration", "fee_manage", "fee_pay_remark",
             "fee_redeem", "fee_subscription", "fee_trust", "investment_range",
             "min_purchase_amount", "min_append_amount", "stop_line", "alert_line",
             "manager_participation_scale", "investment_idea", "structure_hierarchy", "remark"]
    try:
        df_fund = pd.read_excel(filefullpath, sheetname=sheet, index_col=0).T.dropna(how='all')
        df_fund.columns = _cols

        #fund_info中的fund_name为空用fund_full_name填充
        df_fund.loc[df_fund['fund_name'].isnull(), ['fund_name']] = df_fund.loc[df_fund['fund_name'].isnull()][
            'fund_full_name']

        #去重，如果df_fund为空报错
        df_fund.dropna(subset=['fund_name', 'match_group'], how='any', inplace=True)
        if len(df_fund) == 0:
            raise AssertionError('上传的基金产品信息必须包含产品名及参赛组别')

        df_fund.index = range(len(df_fund))

        df_fund['fund_id'] = df_fund['fund_name'].apply(lambda x: helpfunc.md5encoder(uid+x).upper())
        df_fund.loc[:, ["fund_type_issuance", "fund_type_structure", "is_main_fund"]] = df_fund[
            ["fund_type_issuance", "fund_type_structure", "is_main_fund"]].applymap(lambda x: x.split('-')[-1])
        df_fund.loc[:, ["foundation_date"]] = df_fund["foundation_date"].apply(lambda x: date_parser(x) if x else None)
        df_fund.loc[:, ["issue_scale", "asset_scale"]] = df_fund[["issue_scale", "asset_scale"]].applymap(
            lambda x: money_string(x, unit='万') if x else None)

        io.to_sql('fund_info', engine_ht, df_fund)
        msg = '产品信息上传成功'

        names = set(df_fund['fund_name'])
        names_used = set(df_fund.dropna(
            subset=["match_group", "fund_type_strategy", "reg_code", "foundation_date", "fund_name", "fund_full_name",
                    "fund_manager", "fund_manager_nominal", "fund_member", "fund_type_issuance", "fund_type_structure",
                    "issue_scale", "asset_scale", "is_main_fund", "fee_pay", ], how='any')['fund_name'])
        not_used = list(names.difference(names_used))
        if len(not_used):
            msg += ", 请及时补充产品:{}的关键信息".format(','.join(not_used))

        #在temp_msg中增加更新消息
        temp_msg = []
        for i in df_fund.index:
            try:
                temp_msg.append(update_fund(df_fund.loc[[i]], uid, msg))
            except Exception as e:
                temp_msg.append(str(e))
        result = {'succeed': True, 'msg': msg, 'detailed': temp_msg}
    except AssertionError as e:
        result = {'succeed': False, 'msg': str(e)}
    except Exception as e:
        result = {'succeed': False, 'msg': '产品信息解析失败', 'error_info': str(e)}
    return result


#处理时间，如果是时间格式，就整理返回年月日字符串，如果不是分割，返回以可读字符串表示的当地时间
#Python time strptime() 函数根据指定的格式把一个时间字符串解析为时间元组。
#Python time strftime() 函数接收以时间元组，并返回以可读字符串表示的当地时间，格式由参数format决定。
def date_parser(date):
    try:
        _split_by = ["-", ".", "/"]
        if isinstance(date, (dt.date, dt.datetime)):
            return date.strftime("%Y%m%d")
        date = str(date)
        for symbol in _split_by:
            splitted = date.split(symbol)
            if len(splitted) == 3:
                year, month, day = splitted
                return "{year}{month}{day}".format(year=year, month=month, day=day)
        return dt.datetime.strptime(str(date), "%Y%m%d").strftime("%Y%m%d")
    except:
        return None


def money_string(ms, unit):
    if isinstance(ms, str):
        num, digit = split_money_string(ms)
        if len(digit):
            #遍历key，如果di有digit，相除
            for di in _digital.keys():
                if di in digit:
                    return num * (_digital.get(di)/_digital.get(unit))
        else:
            return num
    else:
        return ms


def split_money_string(ms):
    import re
    pattern = "\d*"
    digit = re.sub(pattern, '', ms)
    num = eval(ms.replace(digit, ''))
    return num, digit