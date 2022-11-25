days = [1,3,7,14,30,60,90,120,150,180] #Days to generate targets for

max_day = 270 #break_even_point

internal_dim_cols=['install_date','install_week', 'app_name', 'platform', 'tier', 'installs']  #dimensions we store the data based on


woo_sol_agg_dim_cols=['install_week', 'app_name', 'platform', 'tier'] #dimensions we create the targets based on
tt_agg_dim_cols=['install_week', 'platform', 'app_name']