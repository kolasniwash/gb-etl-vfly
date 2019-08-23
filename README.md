## Automated pipeline to transform batched data into kpis

This project automated the retreval and processing of batched data from adjust.

Summary of project's functions.

1. get_data_verifly connects to the projects google cloud bucket and donaloads 24h worth of batched data. Batched data is held in gzip files at 1h intervals.
2. transform_metrics groups and aggregates the data for the various metrics needed for the project.
3. big-query-upload combines the new data from the current 24h period with the previously processed data stored in google big query. The results of the combination is uploaded back to google big query.
