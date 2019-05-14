if __name__ == "__main__":
	from google.cloud import bigquery
	from google.oauth2 import service_account
	import pandas as pd
	import datetime
	import get_data_verifly as gdv
	import transform_metrics as tm


	'''
	The main script function called daily via a cron on a google VM.
	Function runs the extract, and transform scripts.
	Joins the result with the most recent version from GBQ and reuploads the result
	'''

	#consruct client and login to goolg api
	credentials = service_account.Credentials.from_service_account_file('/home/nick/adjust/keys/vfly/verifly-237116-82a9642924f2.json')
	project_id = 'verifly-237116'
	client = bigquery.Client(credentials = credentials, project = project_id)

	#create two date strings. one for today, one for yesterday
	today = datetime.date.today()
	yesterday = today - datetime.timedelta(days=1)
	today = str(today)
	yesterday = str(yesterday)

	#naming tables
	dataset_id = 'adjust_deliverables'
	table_name = 'vfly-deliverables.csv' #+ today
	table_name_bigquery = "vfly_deliverables"
	local_path = "/home/nick/adjust/data/verifly/deliverables/" + table_name
	print("Local path: " + local_path)

	#download old data as dataframe from google big query
	dataset_ref = client.dataset(dataset_id).table(table_name_bigquery)
	table = client.get_table(dataset_ref)
	data_gbq = client.list_rows(table).to_dataframe()
	print("Downloading previous data from google big query...")


#	#drop the pandas index that is added when uploading to gbq
	data_gbq = data_gbq.iloc[:,1:]
#	data_prev30 = data[data['date'] < last30]


	#details for data extraction
	bucket = "verifly-adjust"
	day = 1
	save_path = '/home/nick/adjust/data/verifly'


	#download and combine batched data from yesterday
	data_yesterday = gdv.get_data_verifly(bucket, day, save_path)

	#transform batched data to metrics and save as csv
	data_yesterday = tm.calc_deliverables(data_yesterday)

	#combine the two csv files. data was saved as csv as a poor solution to merging a multiindexed dataframe
#	data_gbq = pd.read_csv('/home/nick/adjust/data/verifly/deliverables/vfly-deliverables.csv')
#	data_gbq = data_gbq.iloc[:,1:]


#	print('base data ')
#	print(data_gbq.columns) 
#	data_yesterday = pd.read_csv("deliverables_" + str(yesterday) + ".csv")

#	print('data yesterday ')
#	print(data_yesterday.columns)

	data_full = pd.concat([data_gbq, data_yesterday], ignore_index = True)

#	print('full dataset ')
#	print(data_full.columns)

	#save the dataframe as local file
	data_full.to_csv(local_path)

	#try to delete previous table. if failed catch the fail and notify
	try:
		print("Trying to delete..." + table_name_bigquery)
		table_ref = client.dataset(dataset_id).table(table_name_bigquery)
		client.delete_table(table_ref)  # API request
		print("Deleted sucessfully")
	except:
		print("No table named: " + table_name_bigquery)

	#recreate the table with the passed csv
	dataset_ref = client.dataset(dataset_id)
	job_config = bigquery.LoadJobConfig()
	job_config.autodetect = True
	job_config.skip_leading_rows = 1

	with open(local_path, 'rb') as source_file:
    		job = client.load_table_from_file(
        	source_file,
        	table_ref,
        	location='EU',  # Must match the destination dataset location.
        	job_config=job_config)  # API request

	job.result()  # Waits for table load to complete.

	print('Loaded {} rows into project: {} dataset: {} table: {}.'.format(job.output_rows, project_id, dataset_id, table_name_bigquery))

