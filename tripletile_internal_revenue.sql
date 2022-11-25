select 
    installs.install_date,
    DATE_TRUNC('week', installs.install_date) as install_week,
    installs.app_name, 
    installs.platform, 
    country_tier.tier, 
    user_rev.date-installs.install_date as day, 
    sum(user_rev.revenue) as revenue_on_day
from PUBLIC.F_USER_REVENUE user_rev
left join f_installs installs 
    using (install_pk) 
left join d_country_tier country_tier 
    on installs.country_code = country_tier.country_code
where installs.install_date > DATEADD(day, -274, current_date )
and installs.app_name in ('tripletile')
and installs.platform in ('android', 'ios') 
and installs.source != 'Organic'
group by 
    installs.install_date, 
    installs.app_name, 
    installs.platform, 
    country_tier.tier, 
    user_rev.date