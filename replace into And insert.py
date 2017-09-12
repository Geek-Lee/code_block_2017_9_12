import helpfunc, io

_org_info = [
    'org_name', 'org_full_name', 'reg_code', 'reg_time', 'found_date', 'reg_capital', 'real_capital', 'region',
    'profile',
    'address', 'team', 'fund_num', 'is_qualification', 'prize', 'team_scale', 'investment_idea', 'master_strategy',
    'asset_mgt_scale', 'linkman', 'linkman_duty', 'linkman_phone', 'linkman_email']

cols = helpfunc.format_sql_columns(_org_info + ['org_id'], 'tb_oi')

print(cols)
print(_org_info+['org_id'])

#tb_oi.org_name,tb_oi.org_full_name,tb_oi.reg_code,tb_oi.reg_time,tb_oi.found_date,tb_oi.reg_capital,tb_oi.real_capital,tb_oi.region,tb_oi.profile,tb_oi.address,tb_oi.team,tb_oi.fund_num,tb_oi.is_qualification,tb_oi.prize,tb_oi.team_scale,tb_oi.investment_idea,tb_oi.master_strategy,tb_oi.asset_mgt_scale,tb_oi.linkman,tb_oi.linkman_duty,tb_oi.linkman_phone,tb_oi.linkman_email,tb_oi.org_id
#['org_name', 'org_full_name', 'reg_code', 'reg_time', 'found_date', 'reg_capital', 'real_capital', 'region', 'profile', 'address', 'team', 'fund_num', 'is_qualification', 'prize', 'team_scale', 'investment_idea', 'master_strategy', 'asset_mgt_scale', 'linkman', 'linkman_duty', 'linkman_phone', 'linkman_email', 'org_id']

#>>> df.to_dict(orient="split")
#{'index': [0, 1, 2], 'columns': ['class', 'name', 'price'], 'data': [[11, 1, 111], [22, 2, 222], [33, 3, 333]]}

#replace into 跟 insert 功能类似，不同点在于：replace into 首先尝试插入数据到表中， 1. 如果发现表中已经有此行数据（根据主键或者唯一索引判断）则先删除此行数据，然后插入新的数据。 2. 否则，直接插入新数据。
#mysql中常用的三种插入数据的语句:

#insert into表示插入数据，数据库会检查主键（PrimaryKey），如果出现重复会报错；
#replace into表示插入替换数据，需求表中有PrimaryKey，或者unique索引的话，如果数据库已经存在数据，则用新数据替换，如果没有数据效果则和insert into一样；
#REPLACE语句会返回一个数，来指示受影响的行的数目。该数是被删除和被插入的行数的和。如果对于一个单行REPLACE该数为1，则一行被插入，同时没有行被删除。如果该数大于1，则在新行被插入前，有一个或多个旧行被删除。如果表包含多个唯一索引，并且新行复制了在不同的唯一索引中的不同旧行的值，则有可能是一个单一行替换了多个旧行。
#insert ignore表示，如果中已经存在相同的记录，则忽略当前新数据；