from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
import sys

sys.path.append("/opt/airflow/dags/de-etl/py_scripts/")
from py_scripts import parse_sql_sequence, get_files_dict_from_dir, insert_into_table

connection = BaseHook.get_connection("pg_hse")

db_user = str(connection.login)
db_password = str(connection.password)
db_host = str(connection.host)
db_port = str(connection.port)
db_name = str(connection.schema)



def insert():
    import pandas as pd
    import psycopg2 as pg
    import sqlalchemy
    from psycopg2 import extras
    import re

    import os

    dir_path = os.path.dirname(os.path.realpath(__file__))
    read_path = os.path.join(dir_path, "data")
    scripts_path = os.path.join(dir_path, "sql_scripts")
    querries = get_files_dict_from_dir(scripts_path)

    list_files = os.listdir(read_path)
    files_dict = {}
    list_files = [
        (datetime.strptime(re.search(r"\d{8}", file).group(), "%d%m%Y").date(), file)
        for file in list_files
    ]
    _ = [files_dict.setdefault(file[0], []).append(file[1]) for file in list_files]

    engine = pg.connect(
        f"dbname='{db_name}' user='{db_user}' host='{db_host}' port='{db_port}' password='{db_password}'"
    )

    for date in files_dict.keys():

        parse_sql_sequence(querries["prepare_stg"], engine)

        for file in files_dict[date]:
            if file.endswith(".xlsx"):
                df = pd.read_excel(os.path.join(read_path, file))
                if "terminals" in file:
                    insert_into_table(engine, df, "", "zxcv_stg_terminals")
                elif "passport_blacklist" in file:
                    df = df.rename(
                        {"date": "entry_dt", "passport": "passport_num"}, axis=1
                    )
                    insert_into_table(engine, df, "", "zxcv_stg_blacklist")
                os.rename(
                    os.path.join(read_path, file),
                    os.path.join(read_path, file).replace("data", "archive").replace('.xlsx','.backup'),
                )
            elif file.endswith(".txt"):
                if "transactions" in file:
                    df = pd.read_csv(os.path.join(read_path, file), sep=";")
                    df = df.rename(
                        {
                            "transaction_id": "trans_id",
                            "transaction_date": "trans_date",
                            "amount": "amt",
                        },
                        axis=1,
                    )
                    df["amt"] = df["amt"].str.replace(",", ".").astype(float)
                    insert_into_table(engine, df, "", "zxcv_stg_transactions")
                os.rename(
                    os.path.join(read_path, file),
                    os.path.join(read_path, file).replace("data", "archive").replace('.txt','.backup'),
                )

        parse_sql_sequence(
            querries["update_clients"], engine, date.strftime("%Y-%m-%d")
        )
        parse_sql_sequence(
            querries["update_accounts"], engine, date.strftime("%Y-%m-%d")
        )
        parse_sql_sequence(
            querries["update_cards"], engine, date.strftime("%Y-%m-%d")
        )
        parse_sql_sequence(
            querries["update_terminals"], engine, date.strftime("%Y-%m-%d")
        )
        parse_sql_sequence(querries["update_blacklist"], engine)
        parse_sql_sequence(querries["update_transactions"], engine)
        parse_sql_sequence(
            querries["check_fraud"], engine, date.strftime("%Y-%m-%d")
        )
        engine.commit()


default_args = {
    "owner": "vnechaykin",
}

with DAG(
    dag_id="dag_new_",
    default_args=default_args,
    schedule_interval = '0 23 * * *', 
    start_date= datetime(2023, 1, 1),  
    catchup=False,
) as dag:

    t = PythonOperator(
        task_id="insert_postgres",
        python_callable=insert,
    )
