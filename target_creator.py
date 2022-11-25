
import basefunctions as bf
import config
import data_getter as dg 
import pandas as pd
import datetime


########################### Woodoku and solitaire targets ##########################################
def get_woo_sol_targets(woo_sol_internal_data):
    woo_sol_targets_data_filled=bf.fill_missing_days(woo_sol_internal_data, 270, config.internal_dim_cols, 'revenue_on_day').copy() #Fill in any empty days

    woo_sol_targets_data_filled_rel_days=woo_sol_targets_data_filled[
                                                        woo_sol_targets_data_filled['day'].isin(config.days+ [config.max_day])
                                                                    ].copy() #Return only required days

    woo_sol_targets_pivot=bf.create_revenue_pivot(woo_sol_targets_data_filled_rel_days) #pivot table

    woo_sol_targets=bf.create_ratios(woo_sol_targets_pivot, config.max_day, config.days, config.woo_sol_agg_dim_cols) #Create targets

    woo_sol_targets_slice=woo_sol_targets[config.woo_sol_agg_dim_cols+[col for col in woo_sol_targets if 'growth' in col]].copy() #relevant columns

    return woo_sol_targets_slice

########################### tripletile targets ##########################################

def get_tt_targets(tt_internal_data, woo_sol_targets_slice):
    tt_data_filled=bf.fill_missing_days(tt_internal_data, 270, config.internal_dim_cols, 'revenue_on_day').copy() #Fill in any empty days

    tt_targets_pivot=bf.create_revenue_pivot(tt_data_filled, gb=config.tt_agg_dim_cols) #pivot table

    tt_targets_pivot_aug=tt_targets_pivot[
        (tt_targets_pivot['install_week']>=datetime.date(2022,8,1)) & 
        (tt_targets_pivot['install_week']<datetime.date(2022,9,1))
        ]

    tt_matrix_comp = bf.matrix_completion(tt_targets_pivot_aug, 20, config.tt_agg_dim_cols)
    tt_matrix_comp.to_csv('tt_matrix_comp.csv', index=False)
    print('matrix complete')

    tt_targets=bf.create_ratios(tt_matrix_comp, config.max_day, config.days, config.tt_agg_dim_cols) #Create targets
    tt_targets.to_csv('tt_targets.csv', index=False)
    print('ratios calculated')

    aug_woo_sol_targets=woo_sol_targets_slice[woo_sol_targets_slice['install_week']>=datetime.date(2021,8,1)]
    aug_woo_sol_targets=aug_woo_sol_targets[aug_woo_sol_targets['install_week']<datetime.date(2021,9,1)]
    aug_woo_sol_tier_targs=aug_woo_sol_targets.groupby(['tier', 'platform']).mean().reset_index()

    growth_cols = [col for col in woo_sol_targets_slice.columns if 'growth_' in col]
    woo_sol_targets_slice_copy=woo_sol_targets_slice.copy()

    print('merging to woo sol')

    aug_mg=pd.merge(left = woo_sol_targets_slice_copy, right = aug_woo_sol_tier_targs, how='left', on=['platform','tier'], suffixes=['','_aug'])
    aug_mg.to_csv('aug_mg.csv', index=False)
    new_cols=[]
    for col in growth_cols:
        aug_mg[f'{col}_shift']=aug_mg[col]/aug_mg[col+'_aug']
        new_cols.append(f'{col}_shift')
    aug_mg=aug_mg[['install_week', 'app_name', 'platform', 'tier']+new_cols]


    aug_mg_gb=aug_mg.groupby(['install_week', 'platform', 'tier']).mean().reset_index()

    aug_mg_gb['app_name']='tripletile'
    tt_targets_slice=tt_targets[['install_week', 'platform']+growth_cols]
    tt_targets_slice_gb=tt_targets_slice.groupby('platform').mean().reset_index()
    tt_target_merge=pd.merge(left=aug_mg_gb, right=tt_targets_slice_gb, how='left', on=['platform'])

    for col in growth_cols:
      tt_target_merge[col]=tt_target_merge[col]*tt_target_merge[f'{col}_shift']

    tt_targets_final=tt_target_merge[['install_week', 'app_name', 'platform', 'tier']+growth_cols]

    return tt_targets_final





if __name__ == "__main__":
    print('running')
    woo_sol_internal_data, tt_internal_data = dg.get_internal_data()
    get_tt_targets(tt_internal_data)