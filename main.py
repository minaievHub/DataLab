from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import logging
import requests
from io import StringIO
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowSkipException

winners_schema = {
    'edition': 'int',
    'start_date': 'str',
    'winner_name': 'str',
    'winner_team': 'str',
    'distance': 'float',
    'time_overall': 'float',
    'time_margin': 'float',
    'stage_wins': 'float',
    'stages_led': 'float',
    'height': 'float',
    'weight': 'float',
    'age': 'int',
    'born': 'str',
    'died': 'str',
    'full_name': 'str',
    'nickname': 'str',
    'birth_town': 'str',
    'birth_country': 'str',
    'nationality': 'str',
}

stage_data_schema = {
    'edition': 'int',
    'year': 'int',
    'stage_results_id': 'str',
    'rank': 'str',
    'time': 'str',
    'rider': 'object',
    'age': 'float',
    'team': 'str',
    'points': 'float',
    'elapsed': 'str',
    'bib_number': 'float',
}

tdf_stages_schema = {
    'Stage': 'str',
    'Date': 'str',
    'Distance': 'float',
    'Origin': 'str',
    'Destination': 'str',
    'Type': 'str',
    'Winner': 'str',
    'Winner_Country': 'str',
}

#def read_files():
#    raise Exception("Forced error")

def read_files(url, schema):
    try:
        response = requests.get(url)
        response.raise_for_status()
        date_columns = ['start_date', 'born', 'died']
        for col in date_columns:
            if col in schema and schema[col] == 'str':
                data = pd.read_csv(StringIO(response.text), header=0, dtype=schema, na_values=['Unknown'])
                data[col] = pd.to_datetime(data[col], format='%Y-%m-%d', errors='coerce').dt.year.astype('Int64')
        else:
            data = pd.read_csv(StringIO(response.text), header=0, dtype=schema, na_values=['Unknown'])
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при отриманні даних: {e}")
        raise

tdf_stages_url = "https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2020/2020-04-07/tdf_stages.csv"
winners_url = "https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2020/2020-04-07/tdf_winners.csv"
stage_data_url = "https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2020/2020-04-07/stage_data.csv"

def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    winners_data = ti.xcom_pull(task_ids='read_winners_task')
    stages_data = ti.xcom_pull(task_ids='read_tdf_stages_task')

    create_winners_table_sql = """
        CREATE TABLE IF NOT EXISTS winners_data (
            edition INT,
            start_date FLOAT,
            winner_name VARCHAR(255),
            winner_team VARCHAR(255),
            distance FLOAT,
            time_overall FLOAT,
            time_margin FLOAT,
            stage_wins FLOAT,
            stages_led FLOAT,
            height FLOAT,
            weight FLOAT,
            age INT,
            born DATE,
            died DATE,
            full_name VARCHAR(255),
            nickname VARCHAR(255),
            birth_town VARCHAR(255),
            birth_country VARCHAR(255),
            nationality VARCHAR(255)
        )
    """

    copy_winners_data_sql = """
        COPY winners_data FROM STDIN WITH CSV HEADER DELIMITER ',';
    """

    create_stages_table_sql = """
        CREATE TABLE IF NOT EXISTS stages_data (
            edition INT,
            year INT,
            stage_results_id VARCHAR(255),
            rank VARCHAR(255),
            time VARCHAR(255),
            rider VARCHAR(255),
            age FLOAT,
            team VARCHAR(255),
            points FLOAT,
            elapsed VARCHAR(255),
            bib_number FLOAT
        )
    """

    copy_stages_data_sql = """
        COPY stages_data FROM STDIN WITH CSV HEADER DELIMITER ',';
    """


    create_winners_table_task = PostgresOperator(
        task_id='create_winners_table',
        sql=create_winners_table_sql,
        postgres_conn_id='postgres_default',
        dag=dag,
    )

    copy_winners_data_task = PostgresOperator(
        task_id='copy_winners_data',
        sql=copy_winners_data_sql,
        postgres_conn_id='postgres_default',
        dag=dag,
    )

    create_stages_table_task = PostgresOperator(
        task_id='create_stages_table',
        sql=create_stages_table_sql,
        postgres_conn_id='postgres_default',
        dag=dag,
    )

    copy_stages_data_task = PostgresOperator(
        task_id='copy_stages_data',
        sql=copy_stages_data_sql,
        postgres_conn_id='postgres_default',
        dag=dag,
    )

    create_winners_table_task >> copy_winners_data_task
    create_stages_table_task >> copy_stages_data_task

def calculate_age_in_decade(row):
    race_date = pd.to_datetime(row['start_date'], format='%Y-%m-%d', errors='coerce')
    race_year = pd.to_numeric(str(race_date.year)[:4], errors='coerce')

    if pd.isnull(race_year) or race_year < 1900:
        return '<1900'
    elif race_year < 1910:
        return '1900-1910'
    elif race_year < 1920:
        return '1910-1920'
    elif race_year < 1930:
        return '1920-1930'
    elif race_year < 1940:
        return '1930-1940'
    elif race_year < 1950:
        return '1940-1950'
    elif race_year < 1960:
        return '1950-1960'
    elif race_year < 1970:
        return '1960-1970'
    elif race_year < 1980:
        return '1970-1980'
    elif race_year < 1990:
        return '1980-1990'
    elif race_year < 2000:
        return '1990-2000'
    elif race_year < 2010:
        return '2000-2010'
    elif race_year < 2020:
        return '2010-2020'
    else:
        return '>=2020'

def average_age_of_winners_by_decades(**kwargs):
    try:
        winners_data = kwargs['ti'].xcom_pull(task_ids='read_winners_task')
        winners_data['decade'] = winners_data.apply(calculate_age_in_decade, axis=1)
        average_age_by_decade = winners_data.groupby('decade')['age'].mean().reset_index()
        print("Average age of winners by decades:")
        print(average_age_by_decade)
    except Exception as e:
        logging.error(f"Error: {e}")
        raise

def decide_next_task(**kwargs):
    winners_data = kwargs['ti'].xcom_pull(task_ids='read_winners_task')
    average_age = winners_data['age'].mean()
    if average_age > 25:
        raise AirflowSkipException
    else:
        return 'print_winners_in_current_decade'

def print_winners_in_current_decade(**kwargs):
    winners_data = kwargs['ti'].xcom_pull(task_ids='read_winners_task')
    winners_data['decade'] = winners_data.apply(calculate_age_in_decade, axis=1)
    current_decade_winners = winners_data[winners_data['decade'] == '2010-2020']
    print("Winners in the Current Decade:")
    print(current_decade_winners)

def print_winners_in_previous_decade(**kwargs):
    winners_data = kwargs['ti'].xcom_pull(task_ids='read_winners_task')
    previous_decade_winners = winners_data[winners_data['decade'] == '2000-2010']
    print("Winners in the Previous Decade:")
    print(previous_decade_winners)

def print_next_execution_date(**kwargs):
    try:
        next_execution_date = kwargs['next_execution_date']
        logging.info(f"Next execution is scheduled for: {next_execution_date}")
    except Exception as e:
        logging.error(f"Error printing next execution date: {e}")
        raise

default_args = {
    'owner': 'vlad',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 20),
    'email': 'minaievvl@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Airflow_skip_DAG',
    default_args=default_args,
    description='Skipping_DAG',
    schedule_interval=timedelta(days=1),
)


read_winners_task = PythonOperator(
    task_id='read_winners_task',
    python_callable=read_files,
    op_args=[winners_url, winners_schema],
    provide_context=True,
    dag=dag,
)

read_stage_data_task = PythonOperator(
    task_id='read_stage_data_task',
    python_callable=read_files,
    op_args=[stage_data_url, stage_data_schema],
    provide_context=True,
    dag=dag,
)

read_tdf_stages_task = PythonOperator(
    task_id='read_tdf_stages_task',
    python_callable=read_files,
    op_args=[tdf_stages_url, tdf_stages_schema],
    provide_context=True,
    dag=dag,
)

load_data_to_postgres_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag,
)

average_age_task = PythonOperator(
    task_id='average_age_of_winners_by_decades',
    python_callable=average_age_of_winners_by_decades,
    provide_context=True,
    dag=dag,
)

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=decide_next_task,
    provide_context=True,
    dag=dag,
)

print_winners_in_current_decade_task = PythonOperator(
    task_id='print_winners_in_current_decade',
    python_callable=print_winners_in_current_decade,
    provide_context=True,
    dag=dag,
)

print_winners_in_previous_decade_task = PythonOperator(
    task_id='print_winners_in_previous_decade',
    python_callable=print_winners_in_previous_decade,
    provide_context=True,
    dag=dag,
)

print_next_execution_date_task = PythonOperator(
    task_id='print_next_execution_date',
    python_callable=print_next_execution_date,
    provide_context=True,
    trigger_rule="none_failed",
    dag=dag,
)

read_winners_task >> read_stage_data_task >> read_tdf_stages_task >> load_data_to_postgres_task >> average_age_task >> branch_task >> print_winners_in_current_decade_task >> print_winners_in_previous_decade_task >> print_next_execution_date_task
