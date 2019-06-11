import pandas as pd
from verify_metrics import calc_deliverables, data_processing, total_revenue_cohort

def apply_metrics(data):

    # Preprocess + AGG_METRICS + COHORT_METRICS
    data = data_processing(data)
    agg_met = calc_deliverables(data)
    cohort_met = total_revenue_cohort(data)

    # Concatenate both dataframes 
    final_dataframe = pd.concat([agg_met, cohort_met], axis=1)
    final_dataframe = final_dataframe.reset_index()

    last_day = final_dataframe.date.max()   
    last_day_data = final_dataframe[final_dataframe.date == last_day]

    return last_day_data