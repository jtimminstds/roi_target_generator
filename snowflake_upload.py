import config
import pandas as pd
import math
import datetime
from db_connector import cur

def snowflake_upload(df):
    df=df.copy()

    #collect dimensions and revenue measures
    column_subset = ['install_date_af', 'app_name', 'platform', 'tier', 'source', 'campaign', 'campaign_type', 'network_group', 'cost', 'installs'] + \
                    [f'day_{day}' for day in config.days] + \
                    [f'day_{day}_predicted_cumulative_revenue' for day in config.days+[config.max_day]] + \
                    [f'day_{day}_target_cumulative_revenue' for day in config.days+[config.max_day]]
    to_upload = df[column_subset]


    #rename to match snowflake table
    renamed_columns = ['install_date', 'app_name', 'platform', 'tier', 'source', 'campaign', 'campaign_type', 'network_group', 'cost', 'installs'] + \
                    [f'day_{day}_actual_cumulative_revenue' for day in config.days] + \
                    [f'day_{day}_predicted_cumulative_revenue' for day in config.days+[config.max_day]] + \
                    [f'day_{day}_target_cumulative_revenue' for day in config.days+[config.max_day]]
    to_upload.columns= renamed_columns

    #filter to rows containing a prediction
    to_upload = to_upload[to_upload['day_270_predicted_cumulative_revenue'].notnull()].copy()

    # reconfigure pivot table to have 1 row per aggregate dimensions per day

    unpivot=pd.melt(to_upload, 
                            id_vars=['install_date', 'app_name', 'platform', 'tier', 'source', 'campaign', 'campaign_type', 'network_group', 'cost', 
                                        'installs'], 
                            value_vars=[f'day_{day}_actual_cumulative_revenue' for day in config.days] + 
                                    [f'day_{day}_predicted_cumulative_revenue' for day in config.days+[config.max_day]] + 
                                        [f'day_{day}_target_cumulative_revenue' for day in config.days+[config.max_day]]
                                        ).copy()

    unpivot['day']=unpivot['variable'].apply(lambda x: int(x.split('_')[1])) #extract day from variable column
    unpivot['variable']=unpivot['variable'].apply(lambda x: '_'.join(x.split('_')[-3:])) #extract actual variable name

    #revpivot to get each variable for the same day on the same row
    to_upload_pivot=unpivot.pivot(index=['install_date', 'app_name', 'platform', 'tier', 'source', 'campaign', 'campaign_type', 'network_group'
                                        , 'cost', 'installs', 'day'], 
                                columns='variable', values='value').reset_index().copy()
                            

    to_upload_pivot['upload_date']=datetime.datetime.now().date()

    #format string columns for upload
    string_cols = ['install_date', 'app_name', 'platform','tier', 'upload_date', 'source', 'campaign', 'campaign_type', 'network_group']
    for column in string_cols:
        to_upload_pivot[column]=to_upload_pivot[column].apply(lambda x: "'"+str(x).replace("'", "''") + "'")

    #get dataframe ready for upload

    column_q = "("+", ".join(to_upload_pivot.columns)+")" #query for each column in the dataframe to upload

    to_upload_pivot['insert']=to_upload_pivot.apply(lambda row: ("("+ (', ').join([str(x) for x in list(row)]) +")").replace("nan", "NULL"), axis=1) #create insert query for each row

    #assign a batch, to upload in batches of 5000
    to_upload_pivot.reset_index(inplace=True, drop=True)
    to_upload_pivot['rn']=to_upload_pivot.index
    to_upload_pivot['batch']=to_upload_pivot['rn'].apply(lambda x: math.floor(x/10000))

    #loop through batches and upload
    log_name = str(datetime.datetime.now())
    print('logging into', f"log_{log_name}.txt")
    myFile = open(f"log_{log_name}.txt", "a")
    for batch in to_upload_pivot['batch'].drop_duplicates():
        print(batch ,'/', to_upload_pivot['batch'].drop_duplicates().max())
        subset = to_upload_pivot[to_upload_pivot['batch']==batch]
        values= ', \n'.join(subset['insert'].drop_duplicates())
        
        query = f"""
        insert into DS_DEV.seasonal_roi_predictor {column_q}
        values
        {values}
        """
    try:
        cur.execute(query)
        myFile.write('batch' + str(batch) + 'success' + '\n')
    except Exception as e:
        myFile.write('batch'+ str(batch) + str(e) + '\n')
    myFile.close()

