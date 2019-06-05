import pandas as pd
from verify_metrics import calc_deliverables, data_processing, total_revenue_cohort


# Read CSV File
data = pd.read_csv('raw_0509_0501.csv')

# Preprocess + AGG_METRICS + COHORT_METRICS

data = data_processing(data)
agg_met = calc_deliverables(data)
cohort_met = total_revenue_cohort(data)

# Concatenate both dataframes 
final_dataframe = pd.concat([agg_met, cohort_met], axis=1)
final_dataframe.reset_index()

# TODO: Save dataframe and upload it to GCS

