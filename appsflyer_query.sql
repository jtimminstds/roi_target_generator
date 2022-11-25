SELECT 
        date install_date, 
        date_trunc(week, date) install_week,
        app as app_name,
        platform,
        tier,
        pid as source, 
        c as campaign, 
        ngroup.network_group,
        ctype.campaign_type,
        day,
        sum(coalesce(cost,0)) as cost,
        sum(users) as installs,
        sum(coalesce(AF_APP_OPENED_MONETIZED_SUM_CUMULATIVE,0)) +
        sum(coalesce(AF_AD_REVENUE_SUM_CUMULATIVE,0)) +
        sum(coalesce(STORE_PRODUCT_PURCHASE_SUCCESS_SUM_CUMULATIVE,0)) +
        sum(coalesce(AF_PURCHASE_SUM_CUMULATIVE,0)) +
        sum(coalesce(AF_IAP_COINS_SUM_CUMULATIVE,0)) revenue_cumulative
FROM public.APPSFLYER_RETENTIONS af 
left join d_country_tier country_tiers 
on country_tiers.country_code = af.geo
left join public.campaign_type ctype 
on af.c = ctype.campaign 
and af.pid = ctype.source 
left join public.network_groups ngroup 
on af.c = ngroup.campaign 
and af.pid = ngroup.source 
where date >= current_date - 185
and app in ('woodoku','solitaire','tripletile')
and platform in ('ios', 'android')
and pid != 'Organic'
and day in {day_query}
group by 
  date, 
  app, 
  platform, 
  tier,
  day,
  pid,
  c,
  ngroup.network_group,
  ctype.campaign_type