from rest_framework import permissions, status
from rest_framework.response import Response
from rest_framework.decorators import api_view, permission_classes
from sqlalchemy import create_engine
from ht_competition.settings import DATABASES
import pandas as pd
from utils import helpfunc, io

engine_ht = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(
    DATABASES['default']['USER'],
    DATABASES['default']['PASSWORD'],
    DATABASES['default']['HOST'],
    DATABASES['default']['PORT'],
    DATABASES['default']['NAME'],
), connect_args={"charset": "utf8"})
# 引擎

_org_info = [
    'org_name', 'org_full_name', 'reg_code', 'reg_time', 'found_date', 'reg_capital', 'real_capital', 'region',
    'profile',
    'address', 'team', 'fund_num', 'is_qualification', 'prize', 'team_scale', 'investment_idea', 'master_strategy',
    'asset_mgt_scale', 'linkman', 'linkman_duty', 'linkman_phone', 'linkman_email']
# org_info字段

_manager_info = ['user_name', 'sex', 'org_name',
                 'introduction', 'entry_date', 'investment_years', 'education',
                 'duty', 'qualification', 'background', 'is_fund_qualification',
                 'is_core_member', 'resume', 'max_asset_mgt_scale', 'prize']
# manger_info字段

@api_view(http_method_names=['POST', 'GET'])
@permission_classes((permissions.AllowAny,))
def investment_company(request):
    method = request.data.get('method')#获取请求方法
    uid = request.data.get('uid')#获取请求uid
    org_id = helpfunc.md5encoder(uid).upper()#传入org_id
    if method == 'query':#查询
        cols = helpfunc.format_sql_columns(_org_info + ['org_id'], 'tb_oi')
        sql = "SELECT {} FROM org_fund_manager_mapping tb_rfm JOIN org_info tb_oi ON tb_oi.org_id = tb_rfm.org_id " \
              "WHERE tb_rfm.uid='{}'".format(cols, uid)
        org_data = pd.read_sql(sql, engine_ht)
        if len(org_data) != 0:
            org_data.fillna('', inplace=True)
            resp = {'succeed': True, 'org_info': org_data.to_dict(orient='index').get(0), 'org_id': org_id}
        else:
            resp = {'succeed': False}
        return Response(resp, status=status.HTTP_200_OK)
    elif method == 'update':#更新
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
    method = request.data.get('method')
    uid = request.data.get('uid')
    if method == 'query':
        cols = helpfunc.format_sql_columns(_manager_info + ['user_id'], 'tb_mi')
        sql = "SELECT {} FROM org_fund_manager_mapping tb_rfm JOIN manager_info tb_mi ON tb_mi.user_id = tb_rfm.user_id " \
              "WHERE tb_rfm.uid='{}'".format(cols, uid)
        manager_data = pd.read_sql(sql, engine_ht)
        manager_data.set_index('user_id', inplace=True)
        if len(manager_data) != 0:
            manager_ids = manager_data['user_name'].to_dict()
            manager_id = request.data.get('manager_id', list(manager_ids.keys())[0])
            manager_info = manager_data.loc[manager_id].fillna('').to_dict()#通过manager_id定位到manager_info一条字典形式
            resp = {'manager_ids': manager_ids, 'manager_info': manager_info, 'succeed': True}
        else:
            resp = {'succeed': False}
        return Response(resp, status=status.HTTP_200_OK)
    elif method == 'update':
        manager_data = request.data.get('manager_data')
        sql = "SELECT tb_mi.user_id,tb_mi.user_name FROM org_fund_manager_mapping tb_rfm " \
              "JOIN manager_info tb_mi ON tb_mi.user_id = tb_rfm.user_id " \
              "WHERE tb_rfm.uid='{}'".format(uid)
        temp_df = pd.read_sql(sql, engine_ht)
        id_name_mapping = temp_df.set_index('user_name')['user_id'].to_dict()
        import_data = []
        for manager in manager_data:
            if manager['user_name'] not in id_name_mapping.keys():
                manager['user_id'] = helpfunc.md5encoder(uid+manager['user_name']).upper()
            import_data.append(manager)
        df = pd.DataFrame(import_data)
        try:
            io.to_sql('manager_info', engine_ht, df)
            return Response({'succeed': True, 'msg': '基金经理信息添加成功'}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'succeed': False, 'error_info': str(e)}, status=status.HTTP_200_OK)
