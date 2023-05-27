#!/bin/bash

airflow db upgrade
airflow scheduler &
airflow webserver