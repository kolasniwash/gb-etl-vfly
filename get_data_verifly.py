from google.oauth2 import service_account
from google.cloud import storage
import pandas as pd
import datetime
from glob import glob
import os
import shutil

#consruct client and login to goolge api
credentials = service_account.Credentials.from_service_account_file('/home/nick/adjust/keys/vfly/verifly-237116-82a9642924f2.json')
project_id = 'verifly-237116'
client = storage.Client(credentials = credentials, project = project_id)

#code from google. downloads a list of all the files in the bucket
def gcs_list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    storage_client = client #storage.client()
    bucket = storage_client.get_bucket(bucket_name)

    blobs = bucket.list_blobs()

    blob_list = []
    for blob in blobs:
        blob_list.append(blob.name)
    return blob_list

#for a dataframe with input in the format of batched output from adjust batches
#will return a dataframe that separates out the date column for easy filtering
def add_date_column(adataframe):
    frames_split = adataframe.iloc[:,-1].str.split('_', expand = True)
    dates = frames_split[1].str.split('T', expand = True)
    adataframe = pd.concat([adataframe, dates], axis = 1, ignore_index = True)
    adataframe.columns = ['adjust_output', 'date','hour']
    return adataframe

#takes an integer an input and returns the date counting input number of days back
def date_to_pull(anint):
    today = datetime.date.today()
    return str(today - datetime.timedelta(days=anint))


#function to select out a range of files to download
def files_to_download(adataframe, adate):
    adate_file_names = adataframe[adataframe['date'] == adate]
    return adate_file_names.iloc[:,0]


#download the google cloud files that we want to process in this 24h period
def gcs_download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = client #storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

#     print('Blob {} downloaded to {}.'.format(
#         source_blob_name,
#         destination_file_name))

#function to get all the paths of downloaded files
def get_paths(apath):
    return glob(apath + '*')   #get all the files in the path directory

#opens the csv.gz files, decompresses them, and joins them together in a single dataframe.
def open_and_join(alist):
    final_df = pd.DataFrame()
    for f in alist:
        to_add = pd.read_csv(f, compression = 'gzip')
        #print('reading..' + f)
        final_df = pd.concat([final_df, to_add], ignore_index = True)
    print('All files read.')
    return final_df

#main function. inputs are a bucket, a number (representing a day refrenced from today), and a path to save the files.
#pulls and combined 24 worth of batched data from google cloud verifly account.
#files are saved in a folder with the date for when they are pulled.
#returns a dataframe of the 24hours batched files joined together.

def get_data_verifly(bucket, anint, apath):
    #make a dataframe with the list of files we want
    file_list = pd.DataFrame(gcs_list_blobs(bucket))
    #clean date from file names, and reutnr df with file name & date
    file_list = add_date_column(file_list)
    #get date to update
    date = date_to_pull(anint)
    #return the list of files to be processed
    file_list = files_to_download(file_list, date)

    file_path = apath + '/' + date + '/'

    if os.path.exists(file_path):
        shutil.rmtree(file_path)
        os.mkdir(file_path)
    else:
        os.mkdir(file_path)

    for file in file_list:
        save_path = file_path + file
#         print(file)
#         print(save_path)
#         print("")
#         print("")
        gcs_download_blob(bucket, file, save_path)

    data_paths = get_paths(file_path)

    return open_and_join(data_paths)

