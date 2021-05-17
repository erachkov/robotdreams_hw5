# Prerequisites

1. pip install -r requirements.txt

# Configuration Airflow before run DAG

1. add to config plugins_folder = /airflow/plugins in airflow.cfg
2. move plugin folder with operator and DAG to local Airflow
3. create new connection that will be used in DAG
   - http_robot_dreams_data_api  -> http https://robot-dreams-de-api.herokuapp.com
   - postgres_robot_dreams -> connect to DB


# ![image](image.png)K