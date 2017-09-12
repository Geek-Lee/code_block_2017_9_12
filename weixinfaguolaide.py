def handle_uploaded_org(filefullpath, uid, sheet):
    _cols = ["org_name", "org_full_name", "reg_code", "reg_time", "found_date", "reg_capital",
             "real_capital", "region", "profile", "address", "team", "fund_num",
             "is_qualification", "prize", "team_scale", "investment_idea", "master_strategy",
             "remark", "asset_mgt_scale", "linkman", "linkman_duty", "linkman_phone", "linkman_email"]
    try:
        df_org = pd.read_excel(filefullpath, sheetname=sheet, index_col=0).T
        df_org.columns = _cols
        df_org.index = range(len(df_org))
        df_org.loc[:, ['reg_time', 'found_date']] = df_org[['reg_time', 'found_date']].applymap(date_parser)
        df_org.loc[:, ["reg_capital", "real_capital"]] = df_org[["reg_capital", "real_capital"]].applymap(
            lambda x: money_string(x, unit='万'))
        df_org.loc[:, ["asset_mgt_scale"]] = df_org["asset_mgt_scale"].apply(lambda x: money_string(x, unit='亿'))
        df_org.loc[:, ['org_id']] = df_org['org_name'].apply(lambda x: helpfunc.md5encoder(uid).upper())
        io.to_sql('org_info', engine_ht, df_org)
        result = {'succeed': True, 'msg': '投顾公司信息解析成功'}
        except Exception as e:
            result = {'succeed': False, 'msg': '投顾公司信息解析失败', 'error_info': str(e)}
            return Response(result, status=status.HTTP_200_OK)
