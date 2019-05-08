import pandas as pd
import datetime

## support functions
#function that takes a row element and a reference value. if element matches reference return True
def get_token(element, reftoken):
    if element == reftoken:
        return 1

# function to create duplicate column of created_at in YYYY-MM-DD format
def convert_time(series):
    dates = pd.to_datetime(series, unit = 's').dt.strftime('%Y-%m-%d')
	#.dt.tz_localize('UTC').dt.tz_convert('Europe/Madrid').dt.strftime('%Y-%m-%d')
    return dates

#main function that:
# takes input the concatinated 24h periods
# transforms the data to deliverables format
# returns a dataframe for the same period of data

def calc_deliverables(data):
    event_tokens = ['dqbpy2', 'q2lhzd', '8887py', 'k6lltp', '11kmp6', '6fvwhu', 'q7bgno', 'iw95yb', '6krwnk', 'yhkd7a', '7x6hst', 'non85g', 'h9cq9i', 'lpolhj', 'dywnda', '7fu4tb', 'cdsjte', 'nl2389', '19dep2', 'it8r06', 'eclho1', 'kju9xs', 'ftt51m', 'qlqsct', 'jdl7m5', 'd9ta4y', 'rclzke', 'tb8st1', 'nugbix', '78w7wo', 'fx5zkq', 'tphdx4', 'ji9b0w', '7a689n', '21df5t', 'u931an', 'ou513k', 'yrqg8l', '2ip9jc', 's2wj8e', 'q8aac4', 'iiih7b', 'nuwmdo', '8txmnq', '98vjzr', 'hzx2py', 'y4nb8s', 'b8cs6y', 'z9grf1', 'u5vfis', 'moryy9', 'dcfb3c', '813ay4', 'h3qsxx', 'e1u3vz', 'g7xof4', 'bg1l9t', 'vsk09s', 'yotvmp', 'r59n0g', 'uaxeoa', 'k32fim', 'x44bbh', '15300u', 'pcfbpu', 'fwxse7', 'aawkuo', '9gbbae']
    event_headers = ['e_dqbpy2', 'e_q2lhzd', 'e_8887py', 'e_k6lltp', 'e_11kmp6', 'e_6fvwhu', 'e_q7bgno', 'e_iw95yb', 'e_6krwnk', 'e_yhkd7a', 'e_7x6hst', 'e_non85g', 'e_h9cq9i', 'e_lpolhj', 'e_dywnda', 'e_7fu4tb', 'e_cdsjte', 'e_nl2389', 'e_19dep2', 'e_it8r06', 'e_eclho1', 'e_kju9xs', 'e_ftt51m', 'e_qlqsct', 'e_jdl7m5', 'e_d9ta4y', 'e_rclzke', 'e_tb8st1', 'e_nugbix', 'e_78w7wo', 'e_fx5zkq', 'e_tphdx4', 'e_ji9b0w', 'e_7a689n', 'e_21df5t', 'e_u931an', 'e_ou513k', 'e_yrqg8l', 'e_2ip9jc', 'e_s2wj8e', 'e_q8aac4', 'e_iiih7b', 'e_nuwmdo', 'e_8txmnq', 'e_98vjzr', 'e_hzx2py', 'e_y4nb8s', 'e_b8cs6y', 'e_z9grf1', 'e_u5vfis', 'e_moryy9', 'e_dcfb3c', 'e_813ay4', 'e_h3qsxx', 'e_e1u3vz', 'e_g7xof4', 'e_bg1l9t', 'e_vsk09s', 'e_yotvmp', 'e_r59n0g', 'e_uaxeoa', 'e_k32fim', 'e_x44bbh', 'e_15300u', 'e_pcfbpu', 'e_fwxse7', 'e_aawkuo', 'e_9gbbae']

    rev_metrics = ['impression', 'event', 'session', 'install', 'click', 'install_update', 'reattribution', 'revenue']
    # rename the column headers
    data = data.reindex(columns = (data.columns.tolist() + event_headers))

    #remove the {} and format all column headers the same
    data.columns = data.columns.str.replace('{','').str.replace('}',"")

    #in the events column, test each element if it matches one of the token headers. If yes, assign True to the specific token's column.
    for token in event_tokens:
        data[('e_' + token)] = data['event'].apply(get_token, reftoken = token)

    data['date'] = convert_time(data['created_at'])

    activities = ['impression', 'event', 'session', 'install', 'click', 'install_update', 'reattribution']
    for activity in activities:
        data[activity] = data['activity_kind'].apply(get_token, reftoken = activity)

    #how data should be grouped
    grouping = ['date', 'network_name', 'campaign_name', 'adgroup_name', 'creative_name', 'region', 'os_name']
    #metrics to group over
    all_metrics = rev_metrics + event_headers

    #calculating main metrics
    agg_metrics = data.groupby(grouping)[all_metrics].apply(lambda x : x.sum())

    return agg_metrics
