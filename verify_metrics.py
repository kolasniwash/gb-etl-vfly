import pandas as pd
import datetime

## support functions
#function that takes a row element and a reference value. if element matches reference return True
def get_token(element, reftoken):
    if element == reftoken:
        return 1
    return 0

# function to create duplicate column of created_at in YYYY-MM-DD format
def convert_time(series):
    dates = pd.to_datetime(series, unit = 's').dt.strftime('%Y-%m-%d')
	#.dt.tz_localize('UTC').dt.tz_convert('Europe/Madrid').dt.strftime('%Y-%m-%d')
    
    return pd.to_datetime(dates)

#main function that:
# takes input the concatinated 24h periods
# transforms the data to deliverables format
# returns a dataframe for the same period of data


def data_processing(data):
    event_titles = ['builder add [x]', 'builder ai success', 'builder create profile', 'builder date success', 
                    'builder email unlock success', 'builder enter ai', 'builder enter date', 'builder failed gate', 
                    'builder first quote', 'builder hour success', 'builder hourly', 
                    'builder month success', 'builder monthly', 'builder passed gate claims', 
                    'builder passed gate first', 'builder preview certificate email', 'builder remove [x]', 
                    'builder sign up', 'builder vault card', 'carousel flip [x]', 'carousel select company [x]-[y]', 
                    'carousel select group [x]', 'confirmation change payment', 'confirmation scroll', 'details add ai enter', 
                    'details add ai success', 'details cancel enter', 'details cancel success', 'details email',
                    'details email success', 'details extend enter', 'details extend success', 'details share',
                    'details view cert', 'details withdraw enter', 'details withdraw success', 
                    'location unavail email sign up', 'location unavail use demo', 'location unavail use zip', 'menu about',
                    'menu buy', 'menu claim', 'menu contact', 'menu create profile', 'menu failed gate', 'menu help',
                    'menu how', 'menu library', 'menu passed gate claims', 'menu passed gate first', 'menu privacy', 
                    'menu sign up', 'menu terms', 'menu vault card', 'permission accept location', 
                    'permission refuse location', 'phone dialog enter', 'phone dialog send', 'phone dialog verify', 
                    'phone dialog verify failed', 'policy activate', 'profile edit', 'profile enter', 'profile log out', 
                    'profile payment update', 'profile phone dialog verify', 'profile phone dialog verify failed', 
                    'purchase success']
    
    event_tokens = ['dqbpy2', 'q2lhzd', '8887py', 'k6lltp', '11kmp6', '6fvwhu', 'q7bgno', 'iw95yb', '6krwnk', 'yhkd7a',
                    '7x6hst', 'non85g', 'h9cq9i', 'lpolhj', 'dywnda', '7fu4tb', 'cdsjte', 'nl2389', '19dep2', 'it8r06', 
                    'eclho1', 'kju9xs', 'ftt51m', 'qlqsct', 'jdl7m5', 'd9ta4y', 'rclzke', 'tb8st1', 'nugbix', '78w7wo', 
                    'fx5zkq', 'tphdx4', 'ji9b0w', '7a689n', '21df5t', 'u931an', 'ou513k', 'yrqg8l', '2ip9jc', 's2wj8e', 
                    'q8aac4', 'iiih7b', 'nuwmdo', '8txmnq', '98vjzr', 'hzx2py', 'y4nb8s', 'b8cs6y', 'z9grf1', 'u5vfis', 
                    'moryy9', 'dcfb3c', '813ay4', 'h3qsxx', 'e1u3vz', 'g7xof4', 'bg1l9t', 'vsk09s', 'yotvmp', 'r59n0g', 
                    'uaxeoa', 'k32fim', 'x44bbh', '15300u', 'pcfbpu', 'fwxse7', 'aawkuo', '9gbbae']
    event_headers = ['e_' + token for token in event_tokens]
    rev_metrics = ['impression', 'event', 'session', 'install', 'click', 'install_update', 'reattribution', 'revenue']
    
    # rename the column header
    #data = data.reindex(columns = (data.columns.tolist() + event_headers))
    
    #remove the {} and format all column headers the same
    data.columns = data.columns.str.replace('{','').str.replace('}',"")
    
    # drop Unnamed 0 column
    #data = data.drop(['Unnamed: 0'], axis=1)

    #in the events column, test each element if it matches one of the token headers. If yes, assign True to the specific token's column.
    for title, token in zip(event_titles, event_tokens):
        col_name = title + ' (e_' + token + ')'
        data[col_name] = data['event'].apply(get_token, reftoken = token)
    data['date'] = convert_time(data['created_at'])
    
    activities = ['impression', 'event', 'session', 'install', 'click', 'install_update', 'reattribution']
    
    for activity in activities:
        data[activity] = data['activity_kind'].apply(get_token, reftoken = activity)
    
    #how data should be grouped
    grouping = ['date', 'network_name', 'campaign_name', 'adgroup_name', 'creative_name', 'region', 'os_name']
    
    #metrics to group over
    all_metrics = rev_metrics + [title + ' (' + header + ')' for title, header in zip(event_titles, event_headers)]
    
    #wrangle data. remove NaNs from grouping columns
    
    data['campaign_name'] = data['campaign_name'].fillna("unknown")
    data['adgroup_name'] = data['adgroup_name'].fillna("unknown")
    data['creative_name'] = data['creative_name'].fillna("unknown")
    data['region'] = data['region'].fillna("undefined")
   
    return data


def calc_deliverables(data):
    
    event_titles = ['builder add [x]', 'builder ai success', 'builder create profile', 'builder date success', 
                    'builder email unlock success', 'builder enter ai', 'builder enter date', 'builder failed gate', 
                    'builder first quote', 'builder hour success', 'builder hourly', 
                    'builder month success', 'builder monthly', 'builder passed gate claims', 
                    'builder passed gate first', 'builder preview certificate email', 'builder remove [x]', 
                    'builder sign up', 'builder vault card', 'carousel flip [x]', 'carousel select company [x]-[y]', 
                    'carousel select group [x]', 'confirmation change payment', 'confirmation scroll', 'details add ai enter', 
                    'details add ai success', 'details cancel enter', 'details cancel success', 'details email',
                    'details email success', 'details extend enter', 'details extend success', 'details share',
                    'details view cert', 'details withdraw enter', 'details withdraw success', 
                    'location unavail email sign up', 'location unavail use demo', 'location unavail use zip', 'menu about',
                    'menu buy', 'menu claim', 'menu contact', 'menu create profile', 'menu failed gate', 'menu help',
                    'menu how', 'menu library', 'menu passed gate claims', 'menu passed gate first', 'menu privacy', 
                    'menu sign up', 'menu terms', 'menu vault card', 'permission accept location', 
                    'permission refuse location', 'phone dialog enter', 'phone dialog send', 'phone dialog verify', 
                    'phone dialog verify failed', 'policy activate', 'profile edit', 'profile enter', 'profile log out', 
                    'profile payment update', 'profile phone dialog verify', 'profile phone dialog verify failed', 
                    'purchase success']
    
    event_tokens = ['dqbpy2', 'q2lhzd', '8887py', 'k6lltp', '11kmp6', '6fvwhu', 'q7bgno', 'iw95yb', '6krwnk', 'yhkd7a',
                    '7x6hst', 'non85g', 'h9cq9i', 'lpolhj', 'dywnda', '7fu4tb', 'cdsjte', 'nl2389', '19dep2', 'it8r06', 
                    'eclho1', 'kju9xs', 'ftt51m', 'qlqsct', 'jdl7m5', 'd9ta4y', 'rclzke', 'tb8st1', 'nugbix', '78w7wo', 
                    'fx5zkq', 'tphdx4', 'ji9b0w', '7a689n', '21df5t', 'u931an', 'ou513k', 'yrqg8l', '2ip9jc', 's2wj8e', 
                    'q8aac4', 'iiih7b', 'nuwmdo', '8txmnq', '98vjzr', 'hzx2py', 'y4nb8s', 'b8cs6y', 'z9grf1', 'u5vfis', 
                    'moryy9', 'dcfb3c', '813ay4', 'h3qsxx', 'e1u3vz', 'g7xof4', 'bg1l9t', 'vsk09s', 'yotvmp', 'r59n0g', 
                    'uaxeoa', 'k32fim', 'x44bbh', '15300u', 'pcfbpu', 'fwxse7', 'aawkuo', '9gbbae']
    event_headers = ['e_' + token for token in event_tokens]
    rev_metrics = ['impression', 'event', 'session', 'install', 'click', 'install_update', 'reattribution', 'revenue']

#grouping without date.slice data using dau/wau/mau
    grouping = ['tracker', 'network_name', 'campaign_name', 'adgroup_name', 'creative_name', 'region', 'os_name']

    all_metrics = rev_metrics + [title + ' (' + header + ')' for title, header in zip(event_titles, event_headers)]

    
    #calculating main metrics
#    agg_metrics = data.groupby(grouping)[all_metrics].apply(lambda x : x.sum())
 
    yesterday = (datetime.date.today() + datetime.timedelta(days = -1))
    dau_bound_date = str((yesterday + datetime.timedelta(days = -1)))
    wau_bound_date = str((yesterday + datetime.timedelta(days = -7)))
    mau_bound_date = str((yesterday + datetime.timedelta(days = -30)))
    upper_bound_date = str(yesterday)

    # returns a filtered dataframe
    dau = data[data.date.between(dau_bound_date, upper_bound_date)]
    wau = data[data.date.between(wau_bound_date, upper_bound_date)]
    mau = data[data.date.between(mau_bound_date, upper_bound_date)]

    agg_metrics = data.groupby(grouping)[all_metrics].apply(lambda x : x.sum())

    unique_dau = dau.groupby(grouping).agg({'random_user_id': 'nunique'})
    unique_wau = wau.groupby(grouping).agg({'random_user_id': 'nunique'})
    unique_mau = mau.groupby(grouping).agg({'random_user_id': 'nunique'})

    unique_dau.columns = ['dau']
    unique_wau.columns = ['wau']
    unique_mau.columns = ['mau']

    final_dataframe = pd.concat([agg_metrics,unique_dau, unique_wau, unique_mau], axis=1, join_axes=[agg_metrics.index])

    final_dataframe.update(final_dataframe[['dau','wau','mau']].fillna(0))

    return final_dataframe, upper_bound_date


def total_revenue_cohort(data):
    
    data.set_index('random_user_id', inplace=True)

    data['CohortGroup'] = data.groupby(level=0)['date'].min().apply(lambda x: x.strftime('%Y-%m-%d'))
    data.reset_index(inplace=True)
    
    grouping = ['tracker', 'network_name', 'campaign_name', 'adgroup_name', 'creative_name', 'region', 'os_name']

    grouped_data = data.groupby(grouping)

    # count the unique users, orders, and total revenue per Group + Period
    cohorts = grouped_data.agg({'random_user_id': pd.Series.nunique,
                           'revenue': 'sum'})

    # make the column names more meaningful
    cohorts.rename(columns={'random_user_id': 'TotalUsers',
                            'revenue': 'CohortRevenue'}, inplace=True)
    
    return cohorts



