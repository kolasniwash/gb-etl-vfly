if __name__ == "__main__":
	import get_data_verifly as gdf
	import pandas as pd
	import transform_metrics as tm

	bucket = "verifly-adjust"
	day = 3
	save_path = '/home/nick/adjust/data/verifly'

	yesterday_data = gdf.get_data_verifly(bucket, day, save_path)

	test = tm.calc_deliverables(yesterday_data)

	test.to_csv('test.csv')
