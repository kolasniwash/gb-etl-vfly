# Script produces a df with the batched outputs from the previous 24 hours.
# 1. run from cron every 24 hours.
# 2. query file names from yesterday
# 3. extract and merge them into a single df output.

import pandas as pd
import datetime
from glob import glob


#function that splits the date data from the verifly file paths
def date_split(dataframe):
    dataframe = dataframe.iloc[:,-1].str.split('_', expand = True)
    dataframe = dataframe[1].str.split('T', expand = True)
    return dataframe.iloc[:, 0]
    
    #function that returns the paths to extract
# takes a date and a path as inputs
def get_paths(apath, adate):
    all_files_in_path = glob(apath + '*')   #get all the files in the path directory
    all_files_in_path = pd.DataFrame(all_files_in_path)[0].str.split('/', expand = True)  #make df and get specific file names
    pull_file_dates = date_split(all_files_in_path) #get the date from the specific file names
    files_by_date = pd.concat([all_files_in_path, pull_file_dates], axis = 1, ignore_index = True) #append date column
    #filter for the filename that matched the date input
    adate_files = files_by_date[files_by_date.iloc[:,-1] == adate]
    print(apath + adate_files.iloc[:,-2])
    return apath + adate_files.iloc[:,-2]

def open_and_join(alist):
    final_df = pd.DataFrame()
    for f in alist:
        to_add = pd.read_csv(f, compression = 'gzip')
        print('reading..' + f)
        final_df = pd.concat([final_df, to_add], ignore_index = True)
    print('All files read.')
    return final_df

def date_yesterday():
    today = datetime.date.today()
    return str(today - datetime.timedelta(days=1))
    
def get_yesterday_csvs(apath):
    yesterday = date_yesterday()
    csv_paths = get_paths(path, yesterday)
    return open_and_join(csv_paths)
