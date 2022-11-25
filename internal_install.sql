select 
    date as install_date,
    app as app_name, 
    platform, 
    country_tier.tier, 
    sum(users) as installs
from PUBLIC.appsflyer_retentions as af
left join d_country_tier country_tier 
    on af.geo = country_tier.country_code
where date >='2021-04-01'
and app in ('woodoku', 'solitaire', 'tripletile')
and platform in ('android', 'ios') 
and pid != 'Organic'
and day=0
group by 
    date, 
    app, 
    platform, 
    country_tier.tier