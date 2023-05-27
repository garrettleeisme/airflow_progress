#!/bin/bash

docker build -t airflow-image:0.1 .
docker run -d -p 8080:8080 -v "$(pwd)":/root/airflow/dags --name airflow-container airflow-image:0.1
