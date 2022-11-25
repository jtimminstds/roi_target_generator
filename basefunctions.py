import matplotlib.pyplot as plt
import seaborn as sns
sns.set()
import datetime
import pandas as pd
import numpy as np




def get_data(query, cursor):
    '''
    execute sql query
    '''
    raw_data=cursor.execute(query).fetchall()  
    num_fields = len(cursor.description)
    field_names = [i[0].lower() for i in cursor.description]
    df = pd.DataFrame(raw_data)
    df.columns = field_names
    return df

def create_revenue_pivot(df, gb=None):
    '''
    pivot revenue columns
    '''
    df=df.copy()
    df['day']=df['day'].apply(lambda x: f'day_{x}')
    index_cols = [col for col in df.columns if col not in ['day', 'revenue_cumulative']]
    df_pivot=df.pivot(index=index_cols, columns=['day'], values='revenue_cumulative').reset_index()
    if gb:
        df_pivot=df_pivot.groupby(gb).sum().reset_index()
    return df_pivot

def create_ratios(df, max_day, target_days, aggregrate_columns):
    '''
    create growth columns
    '''
    all_days = target_days + [max_day]
    cum_rev_cols = [f'day_{day}' for day in all_days]

    gb=df.groupby(aggregrate_columns).sum(numeric_only=True).reset_index()[aggregrate_columns + cum_rev_cols]
    for day in target_days:
        gb[f'growth_{day}_to_{max_day}']=gb[f'day_{max_day}']/gb[f'day_{day}']
    return gb


def fill_missing_days(df, max_day, dim_cols, revenue_col, create_cumulative_revenue=True):
    '''
    cohorts can miss days where no revenue was made, 
    this function fills those in with 0s
    '''
    days = pd.DataFrame(data=[[i,1] for i in range(0,max_day+1)], columns=['day', 'jk'])

    dims=df[dim_cols].drop_duplicates().copy()
    dims['jk']=1

    all_days=pd.merge(left=dims, right=days, how='left', on =['jk'])
    all_days['activity_date']=all_days.apply(lambda row: row['install_date']+datetime.timedelta(days=row['day']), axis=1)
    all_days=all_days[all_days['activity_date']<datetime.datetime.now().date()]
    all_days.drop('jk', axis=1, inplace=True)
    all_days.drop('activity_date', axis=1, inplace=True)
    all_days_combine=pd.merge(left=all_days, right=df, how='left', on =list(all_days.columns))
    all_days_combine[revenue_col]=all_days_combine[revenue_col].fillna(0)
    all_days_combine['revenue_cumulative'] = all_days_combine.sort_values(by=dim_cols+['day'], ascending=True)\
                        .groupby(dim_cols)[revenue_col]\
                        .cumsum()
    all_days_combine.drop(revenue_col, axis=1 , inplace=True)

    return all_days_combine


def get_most_recent_day(row, days):
    '''
    from the list of days pick the most recent for which the cohort has the actual 
    data
    '''
    actuals = {i:float(row[f'day_{i}']) for i in days if pd.notnull(row[f'day_{i}'])}
    most_recent_day = max([i for i in actuals.keys()])
    return most_recent_day


def calculate_cumulative_revenue(row, days, max_day, day):
    '''
    predicted cumulative revenue:
    if the cohort has actuals for the day, use the day, 
    if not calculate the growth rate from the most recent value to the specified
    day
    '''
    most_recent_day = get_most_recent_day(row, days)
    most_recent_val = row[f'day_{most_recent_day}']
    if most_recent_day >=day:
        most_recent_below = max([d for d in days if d<=day])
        return float(row[f'day_{most_recent_below}']) if float(row['cost'])>0 else None
    if day == max_day:
        scale_factor = row[f'growth_{most_recent_day}_to_{max_day}']
    else:
        scale_factor = row[f'growth_{most_recent_day}_to_{max_day}']/row[f'growth_{day}_to_{max_day}']

    return (float(scale_factor)*float(most_recent_val))

def matrix_completion(df, start_day, agg_cols):
    day_cols = [f'day_{i}' for i in range(0,271) if f'day_{i}' in df.columns]

    diff_matrix=df[agg_cols].copy()
    for col_lower, col_higher in zip(day_cols[:-1], day_cols[1:]):
        diff_matrix[f'{col_higher}-{col_lower}']=df[col_higher]-df[col_lower]
        diff_matrix.loc[diff_matrix[f'{col_higher}-{col_lower}']<=0,f'{col_higher}-{col_lower}']=np.nan
        diff_matrix=diff_matrix.copy()

    frac_matrix=df[agg_cols].copy()
    for col_lower, col_higher, col_max in zip(day_cols[:-2], day_cols[1:-1], day_cols[2:]):
        frac_matrix[f'decay_{col_lower}_{col_higher}_{col_max}']=diff_matrix[f'{col_max}-{col_higher}']/diff_matrix[f'{col_higher}-{col_lower}']

        frac_matrix=frac_matrix.copy()

    fig, axs= plt.subplots(len(frac_matrix),1, figsize=(8,8*len(frac_matrix)))
    j=0
    avgs=[]
    for i, row in frac_matrix.iterrows():
        decay_cols=[col for col in row.keys() if 'decay_' in col][start_day:]
        vals=list(row[decay_cols].values)
        axs[j].scatter(x=[k for k in range(len(decay_cols))],y=vals)
        avg = sum([x for x in vals if x==x])/len([x for x in vals if x==x])
        avgs.append(avg)
        axs[j].set_title(str(row['install_week']) + ' ' +row['platform'] + ' - - - - '+ str(avg))

        j+=1

    plt.savefig('foo.pdf')

    decay_val = sum(avgs)/len(avgs)
    print('decay_val is', decay_val)

    diff_cols = [col for col in diff_matrix if 'day_' in col]
    for curr_col, prior_col in zip(diff_cols[1:], diff_cols[:-1]):
        diff_matrix.loc[diff_matrix[curr_col].isnull(),curr_col]=diff_matrix.loc[diff_matrix[curr_col].isnull(),prior_col]*decay_val

    new_totals=df[agg_cols+['installs', 'day_0']].copy()
    day=1
    while day<=270:
        new_totals[f'day_{day}']=new_totals[f'day_{day-1}']+diff_matrix[f'day_{day}-day_{day-1}']
        new_totals=new_totals.copy()
        day+=1

    return new_totals