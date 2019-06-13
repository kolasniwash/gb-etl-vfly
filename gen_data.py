
import get_data_verifly as gdf
import pandas as pd
import datetime
										#
def gen_data(bucket, num_days, save_path):
	#bucket = "verifly-adjus"
	#day = 3
	#save_path = '/home/nick/adjust/data/verifly'

	metrics = []

	for day in range(1,num_days+1):
		data = gdf.get_data_verifly(bucket, day, save_path)
		metrics.append(data)

	data_metrics = pd.concat(metrics, ignore_index = True)
	#data_metrics.to_csv('/home/nick/adjust/data/verifly/deliverables/vfly_deliverables_raw.csv')
	return data_metrics

