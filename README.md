# Motor-Vehicles-Collision-Data-Analysis-NewYork
### Identifying High-Risk Areas based on accident data.

## Project about 
It's course project at data-engineering-zoomcamp by [DataTalksClub](https://github.com/DataTalksClub/data-engineering-zoomcamp).

## Problem: Identifying High-Risk Areas
For this project I've tried to build a batch pipeline to process motor vehicle collisions data in New York from ('https://catalog.data.gov/',An official website of the GSA's Technology Transformation Services).The Motor Vehicle Collisions crash table contains details on the crash event. Each row represents a crash event. The Motor Vehicle Collisions data tables contain information from all police reported motor vehicle collisions/accidents in NYC. 
Accidents can occur more frequently in certain neighborhoods or zip codes. Identifying these high-risk areas is crucial for improving safety measures, allocating resources effectively, and preventing accidents. **We want to pinpoint the locations where accidents are most likely to happen.**

## Dataset
[Motor Vehicle Collisions crash dataset website](https://catalog.data.gov/dataset/motor-vehicle-collisions-crashes/resource/b5a431d2-4832-43a6-9334-86b62bdb033f)

[Motor Vehicle Collisions crash dataset direct link](https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD)

## Technologies
- **Google cloud platform** (GCP):
  - VM Instance to run project on it.
  - Cloud Storage to store processed data.
  - BigQuery as data source for dashboard.
- **Terraform** to create cloud infrastructure.
- **Airflow** to run data pipelines as DAGs.
- **PySpark** to pre-process raw data.
- **dbt** to do analytics engineering. 
- **Google data studio** to visualize data.

# Results 
## Cloud infrastructure
Except the VM Instance, all project infra setups with terraform: 
- Data Lake for all of the project data.
- BigQuery for transformed data tablels as source for dashboard.

## Data pipelines
The dataset data download, process and upload to cloud storage, transfer to data warehouse is done via these Airflow DAGs:
**Local to GCS Dag** 
  - Runs once since there is a single dataset, can be changed accordingly though. 
  - Downloads the dataset file in the csv format. This task runs by a bash script, which downloads the data. 
  - Next the data is pre-processed using pyspark(changing column names, data types, etc) and saves it locally in the form of parquet file. 
  - This file is then uploaded to project Cloud Storage(Data Lake).
  - Last task triggers the <code>gcs_to_bq_dag</code> so that it runs right after the data has been loaded to project Cloud Storage.

 **GCS to BQ Dag**
  - The dag transfers the data in parquet files in the project Cloud Storage to the project BigQuery dataset made earlier using terraform.
  - Followed by creation of a partitioned and clustered table at project BigQuery dataset.
  - Lastly local clean up is done to erase the data from the local system.

## Analytics Engineering (dbt) - 
Please refer [here](https://github.com/adityachaudhary99/Motor-Vehicles-Collision-Data-Analysis-NewYork/blob/5_dbt/README.md)

## Dashboard
Simple dashboard at Google Data studio with few graphs.
- .
- .

# How to run project? 
Project was build on GCP Debian VM Instance, so you can find code snippets for these particular case [here](https://github.com/adityachaudhary99/Motor-Vehicles-Collision-Data-Analysis-NewYork/blob/main/pre-reqs.md).

## Prereqs
- Anaconda
- Docker + Docker-compose
- GCP project
- Terraform

## Setup & Deploy
1. Create cloud infrasctructure via Terraform. Look at instructions at [terraform dir](https://github.com/adityachaudhary99/Motor-Vehicles-Collision-Data-Analysis-NewYork/tree/main/2_terraform).
2. Run Airflow in docker and trigger DAGs. Look at instructions at [airflow dir](https://github.com/adityachaudhary99/Motor-Vehicles-Collision-Data-Analysis-NewYork/tree/main/3_airflow/airflow).
3. Connect Google Data Studio dashboard to project BigQuery as a source.
