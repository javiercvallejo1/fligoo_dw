Hey there!

This is a simple ETL implementation for flight data using Airflow as orchestrator. The main idea is simple:

-Make request to API https://aviationstack.com/ and save json file
-read json file and extract fields of interest, also, make some clean up on fields.
-load data into postgres data warehouse on 'processed' layer under table fligootest


if you, dear reader, for some reason feel the necessity to collect data from real-time flights to make your own analysis, please, follow the steps bellow:

1. Install your kubernetes environment. if you already have Docker, you can use Kubernetes cluster provided with your instalation, you can use Minikube as well.
2. set up Airflow on top of your cluster, for this, you can make use of official Airflow Helm chart. also, i have provided a custom Airflow image in case you need it since it contains all requirements for the DAG to run successfully.
3. you have to configure airflow-values to listen to this Git repository. fortunately, the provided YAML on k8s folder is already setup
4. for postgres datawarehouse, just build the image as usual. usable through standard connection (localhost:5432, username :postgres, pss: admin,db:processed)


There's also a file PoC provided on airflow folder called request_to_api.py. It contains all necessary functions and configurations to run the pipeline on your local machine (without using airflow)



