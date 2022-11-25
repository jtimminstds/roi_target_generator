from db_connector import cur
import basefunctions as bf
import config
import pandas as pd

def get_af_data():
        day_query='('+', '.join([str(day) for day in config.days]) +')'


        with open("appsflyer_query.sql", "r", encoding="utf-8") as query_file:
                af_query = query_file.read()
        af_query = af_query.replace("{day_query}",day_query)
        af_data=bf.get_data(af_query, cur)
        af_pivot=bf.create_revenue_pivot(af_data)
        return af_pivot

def get_internal_data():

        with open("woodoku_solitaire_internal_revenue.sql", "r", encoding="utf-8") as query_file:
                woo_sol_rev_query = query_file.read()
        woo_sol_rev_raw=bf.get_data(woo_sol_rev_query, cur)

        with open("tripletile_internal_revenue.sql", "r", encoding="utf-8") as query_file:
                tt_rev_query = query_file.read()
        tt_rev_raw=bf.get_data(tt_rev_query, cur)

        with open("internal_install.sql", "r", encoding="utf-8") as query_file:
                installs_query = query_file.read()
        installs_data=bf.get_data(installs_query, cur)

        woo_sol_internal_data=pd.merge(left=woo_sol_rev_raw, right=installs_data, how='left', on=['install_date', 'app_name', 'platform', 'tier'])
        woo_sol_internal_data=woo_sol_internal_data[woo_sol_internal_data['installs']>50] #remove volatile cohorts

        tt_internal_data=pd.merge(left=tt_rev_raw, right=installs_data, how='left', on=['install_date', 'app_name', 'platform', 'tier'])
        tt_internal_data=tt_internal_data[tt_internal_data['installs']>50] #remove volatile cohorts
        return woo_sol_internal_data, tt_internal_data