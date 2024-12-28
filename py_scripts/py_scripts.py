def insert_into_table(engine, df, db="", table_name=""):
    from psycopg2 import extras
    import pandas as pd

    if len(df) > 0:
        columns = ",".join(df.columns)
        values = "VALUES({})".format(",".join(["%s" for _ in df.columns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(table_name, columns, values)
        cur = engine.cursor()
        extras.execute_batch(cur, insert_stmt, df.values)
        cur.close()


def get_files_dict_from_dir(dir_path: str):
    import os
    from os import listdir

    file_dict = {}
    for file in listdir(dir_path):
        with open(os.path.join(dir_path, file)) as file_object:
            file = file.replace(".sql", "")
            file_dict[file] = file_object.read()

    return file_dict


def execute_sql(querry, connection):
    with connection.cursor() as cursor:
        cursor.execute(querry)


def parse_sql_sequence(querry, connection, insert_date=""):
    queries = querry.split(";")
    for query in queries:
        q = query.strip().strip(";").strip().format(insert_dt=insert_date)
        if q == "":
            continue
        else:
            execute_sql(q, connection)
