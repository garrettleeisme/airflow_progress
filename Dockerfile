FROM ubuntu:20.04

RUN apt update
RUN apt install -y python3
RUN apt install -y python3-pip

RUN pip3 install apache-airflow==2.6.0
RUN pip3 install apache-airflow[postgres]
RUN pip3 install apache-airflow-providers-sftp[ssh]
RUN pip3 install psycopg2-binary
# RUN pip3 install sqlalchemy==1.4.48
# RUN pip3 install pandas==2.0.1
# RUN pip3 install scikit-learn==1.2.2
# RUN pip3 install tomli==2.0.1
# RUN pip3 install pyarrow==12.0.0

WORKDIR /root

RUN airflow users create \
    --username group13_2 \
    --firstname Garrett \
    --lastname Lee \
    --role Admin \
    --email garrettlee2024@northwestern.edu \
    --password my_password 

COPY connections.json /root/airflow/connections.json
COPY /dags/sample_dag.py /root/airflow/dags/sample_dag.py

RUN airflow db init

EXPOSE 8080

CMD ["airflow", "webserver", "-p", "8080"]