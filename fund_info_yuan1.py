import re
import pandas as pd
from sqlalchemy import create_engine

ENGINE_EASY = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('gtja01', 't6G375Kd', 'db.chfdb.cc', 4171, 'test_gt', ),
    connect_args={"charset": "utf8"})


def fetch_fund_info(fund_id, engine=ENGINE_EASY):
    #if isinstance(fund_id, str):
        #fund_id = [fund_id]
    #fund_id = ','.join(["'{}'".format(ids) for ids in fund_id])
    sql = "SELECT fia.fund_id,DATE_FORMAT(fia.statistic_date, '%%Y-%%m-%%d') AS nav_date," \
          "fia.fund_name,fia.fund_full_name,fia.type_code_name_3 " \
          "AS fund_type_issuance,concat_ws('-',fia.type_code_name_1,fia.stype_code_name_1) AS fund_type_strategy," \
          "DATE_FORMAT(fia.foundation_date, '%%Y-%%m-%%d') AS foundation_date," \
          "fia.type_code_name_4 AS fund_type_structure,fia.region," \
          "fia.fund_status,fia.fund_time_limit,fia.open_date,fia.data_freq,fia.fund_stockbroker," \
          "fia.fund_custodian,fia.fee_subscription,fia.expected_return,fia.fee_redeem,fia.fee_manage," \
          "fia.fee_trust,DATE_FORMAT(fia.reg_time, '%%Y-%%m-%%d') AS reg_time," \
          "fia.fee_pay,mi.user_name AS fund_manager,mi.resume AS manager_info," \
          "oi.`profile` AS org_info, fia.org_full_name AS org_name,fas.asset_scale FROM fund_info_aggregation fia " \
          "LEFT JOIN fund_manager_mapping fmm ON fmm.fund_id = fia.fund_id " \
          "AND fmm.is_leader = 1 AND fmm.is_current = 1 " \
          "LEFT JOIN manager_info mi ON fmm.user_id = mi.user_id LEFT JOIN org_info oi ON oi.org_id = fia.org_id " \
          "LEFT JOIN fund_asset_scale fas ON fas.fund_id = fia.fund_id AND fas.statistic_date IN " \
          "(SELECT min(statistic_date) FROM fund_asset_scale WHERE fund_id = fia.fund_id) " \
          "WHERE fia.fund_id = '{}'".format(fund_id)
    data = pd.read_sql(sql, engine)
    print(data)
    result = {}
    for fid in set(data['fund_id']):
        info_data = data[data['fund_id'] == fid]
        info_data.drop('fund_id', axis=1, inplace=True)
        info_data.index = range(len(info_data))
        manager_data = info_data[['fund_manager', 'manager_info']]
        manager_data.drop_duplicates(subset=['fund_manager'], inplace=True)
        org_data = info_data[['org_name', "org_info"]]
        org_data.drop_duplicates(inplace=True)
        info_data.drop(['fund_manager', 'manager_info', 'org_name', "org_info"], axis=1, inplace=True)
        info_data.drop_duplicates(inplace=True)
        manager_info = []
        for i in range(len(manager_data)):
            manager_name = manager_data['fund_manager'][i]
            manager_resume = manager_data['manager_info'][i]
            if manager_name is not None and manager_resume is not None:
                pattern = manager_name + "\w{0,2}\s*[：:,，]\s*"
                manager_resume = re.sub(pattern, "", manager_resume)
            manager_info.append({"manager_name": manager_name, "manager_resume": manager_resume})
        info_data['manager_info'] = [manager_info]
        info_data['org_info'] = org_data.to_dict(orient='record')
        result[fid] = info_data.to_dict(orient='record')[0]
    return result


fetch_fund_info('JR000004')
