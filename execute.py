import basefunctions as bf 
import config 
import data_getter as dg 
import target_creator as tc
import pandas as pd
import datetime
import snowflake_upload as su

print('gathering data')

woo_sol_internal_data, tt_internal_data = dg.get_internal_data()

print('generating targets')
print('woo sol')
woo_sol_targets = tc.get_woo_sol_targets(woo_sol_internal_data)

print('tt')

tt_targets = tc.get_tt_targets(tt_internal_data, woo_sol_targets)

final_targets=pd.concat([woo_sol_targets, tt_targets])
final_targets.to_csv('final_targets.csv', index=False)
print('retrieving af data')

af_pivot = dg.get_af_data()

print('calculating predictions')

#create columns to join install_week 2022 to install_week 2021
af_pivot['install_date_2021']=af_pivot['install_date'].apply(lambda x: datetime.date(2021, x.month, x.day))
af_pivot['install_week_2021']=af_pivot['install_date_2021'].apply(lambda x: x-datetime.timedelta(days=x.isoweekday()-1))

af_pivot.rename(inplace=True, columns = {'install_date':'install_date_af', 'install_week':'install_week_af'}) #rename old install date and week columns

internal_af_merge=pd.merge(left=af_pivot, right=final_targets, how='inner', 
                        left_on=['install_week_2021', 'app_name', 'platform', 'tier'], 
                        right_on=['install_week', 'app_name', 'platform', 'tier'])

#calculate predicted revenues and target revenues
for day in config.days + [config.max_day]:
    print(day)

    internal_af_merge[f'day_{day}_predicted_cumulative_revenue']= internal_af_merge.apply(
                                                bf.calculate_cumulative_revenue, axis=1, days=config.days, max_day=config.max_day, day=day
                                                )

    if day != config.max_day:
        internal_af_merge[f'day_{day}_target_cumulative_revenue']=internal_af_merge.apply(
                        lambda row: float(row['cost'])/float(row[f'growth_{day}_to_{config.max_day}']) if row['cost']>0 else None, axis=1
                        )

    internal_af_merge[f'day_{config.max_day}_target_cumulative_revenue']=internal_af_merge['cost'] #target at day 270 is to reach cost

internal_af_merge.to_csv('internal_af_merge.csv', index=False)

print('starting upload')

su.snowflake_upload(internal_af_merge)

