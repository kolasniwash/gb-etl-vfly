if __name__ == "__main__":
	import get_data_verifly as gdf
	import pandas as pd
	import transform_metrics as tm
	import datetime
#
	bucket = "verifly-adjust"
#	day = 3
	save_path = '/home/nick/adjust/data/verifly'
#	days3 = datetime.date.today() - datetime.timedelta(days = 3)
#	days2 = datetime.date.today() - datetime.timedelta(days = 2)

	days = [2,3,4,5,6,7,8,9,10,11,12,13,14]
	metrics = []

	for day in days:
		data = gdf.get_data_verifly(bucket, day, save_path)
		metric = tm.calc_deliverables(data)
		metrics.append(metric)

	data_metrics = pd.concat(metrics, ignore_index = True)
	data_metrics.to_csv('vfly-deliverables.csv')
