import io
import csv

from airflow.providers.postgres.hooks.postgres import PostgresHook

DB_HOOK = PostgresHook('aib-db')
CONN = DB_HOOK.get_conn()
CURSOR = CONN.cursor()

def get_pk_value(pk_table,pk_feature,fk_feature,fk_value):
    """
    pk_table 에서 fk_feature 를 찾아 ID 를 반환합니다.
    """
    sql = f"SELECT {pk_feature} FROM {pk_table} WHERE {fk_feature} = \'{fk_value}\'"
    print("sql : ", sql)
    result = DB_HOOK.get_first(sql = sql)

    if result == None:
        print('More than one IDs were found')
        statusCode, body = 500, "More than one IDs were found"
        return None
    
    return result[0]


def insert_csv_to_database(table, csv_data):
    """
    csv_data를 Database table 에 삽입합니다.
    """
    csv_data = io.StringIO(csv_data)
    print("csv_data : ",csv_data)
    column = ''

    for row in csv.DictReader(csv_data):

        column = ','.join(list(row.keys()))
        row = ','.join(list(row.values()))
        try:
            sql = f"INSERT INTO {table} ({column}) VALUES ({row})"
            print("sql : ",sql)
            result = DB_HOOK.run(sql=sql, autocommit=True)
 
        except:
            import traceback
            print(f"Failed to insert data into {table}")
            print(traceback.format_exc())
            print(row)
            statusCode, body = 500, "Failed to insert data"
            raise Exception
    
        statusCode, body = 200, "Successfully inserted datas"
        print("Successfully inserted datas")

    return True