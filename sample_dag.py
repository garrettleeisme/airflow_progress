'''
DE 300 Project - Airflow Module
Names: Brooke Leber and Garrett Lee
Date: June 2, 2023
'''

# 0. Import packages and modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from airflow.operators.subdag_operator import SubDagOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import boto3


# --------------------------------------------------------- #
# 1. Parameters
TABLE_NAMES = {
    "original_data": "columns",
    "transformed_data": "columns_transformed"
}

ENCODED_SUFFIX = "_encoded"

default_args = {
    'owner': 'group13',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

BUCKET_NAME = "de300-group13-airflow"

# --------------------------------------------------------- #
# 2. Functions

def from_table_to_df(input_table_names: list[str], output_table_names: list[str]):
    """
    Decorator to load data from S3 into dataframes, pass the dataframes to the function, and write the resulting dataframes back to S3.

    The function must return a dictionary with keys 'dfs', where the value is a list of dictionaries with keys 'df' and 'table_name'. Each dataframe is written to the corresponding S3 object specified by 'table_name'.
    """
    import pandas as pd
    def decorator(func):
        def wrapper(*args, **kwargs):
            """
            Load tables from S3 to dataframes
            """
            if input_table_names is None:
                raise ValueError('input_table_names cannot be None')

            s3 = boto3.client('s3')

            print(f'Loading input tables from S3: {input_table_names}')

            # Read tables from S3 and convert to dataframes
            dfs = []
            for table_name in input_table_names:
                obj = s3.get_object(Bucket=BUCKET_NAME, Key=table_name)
                df = pd.read_csv(obj['Body'])
                dfs.append(df)

            if isinstance(input_table_names, str):
                dfs = dfs[0]
            # At this point, dfs is a collection of dataframes

            """
            Call the main function
            """
            kwargs['dfs'] = dfs
            kwargs['output_table_names'] = output_table_names
            result = func(*args, **kwargs)

            """
            Write dataframes in result to S3 objects
            """
            print(f'Writing dataframes to S3: {output_table_names}')
            for pairs in result['dfs']:
                df = pairs['df']
                table_name = pairs['table_name']
                s3.put_object(Body=df.to_csv(index=False), Bucket=BUCKET_NAME, Key=table_name)
                print(f'Wrote dataframe to S3 object: {table_name}')

            result.pop('dfs')

            return result
        return wrapper
    return decorator

def process_and_save_file_to_s3():
    s3_bucket = Variable.get("s3_bucket")
    s3_key = Variable.get("s3_key")
    local_file = Variable.get("local_file")

    # Perform your file processing operations here
    # Save the processed file to the local_file path

    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_file(filename=local_file, key=s3_key, bucket_name=s3_bucket, replace=True)
    print("File saved to S3 successfully.")

@from_table_to_df(TABLE_NAMES['original_data'], None)
def multiply_all_predictors(**kwargs):
    '''
    Create a new column with all predictors multiplied
    '''
    import pandas as pd

    data_df = kwargs['dfs']

    data_df['Multiplied'] = data_df["Predictor1"] * data_df["Predictor2"] * data_df["Predictor3"]

    return {
    'dfs': [
        {'df': data_df, 
         'table_name': TABLE_NAMES['transformed_data']
         }]
    }


# --------------------------------------------------------- #
# 3. Define DAG
dag = DAG(
    'Simple-Trial',
    default_args=default_args,
    description='Multiply two columns to get a third column',
    schedule_interval="@daily",
    tags=["de300"]
)

# --------------------------------------------------------- #
# 4. Define operators
list_bucket_operator = PythonOperator(
    task_id='list_buckets',
    python_callable=process_and_save_file_to_s3,
    provide_context=True,
    dag=dag
)

multiply_all_predictors_operator = PythonOperator(
    task_id='multiply_all_predictors',
    python_callable=multiply_all_predictors,
    provide_context=True,
    dag=dag
)

# --------------------------------------------------------- #
# 5. Define the order of steps for Airflow
list_bucket_operator >> multiply_all_predictors_operator

