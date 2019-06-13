import pandas as pd
from verify_metrics import calc_deliverables, data_processing, total_revenue_cohort

def apply_metrics(data):

    # Preprocess + AGG_METRICS + COHORT_METRICS
    data = data_processing(data)
    data_with_id = data[~data.random_user_id.isna()]
    agg_met, yesterday_date = calc_deliverables(data)
    cohort_met = total_revenue_cohort(data)

    agg_met['date'] = yesterday_date 
    return agg_met, cohort_met

    