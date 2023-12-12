import duckdb
import os
import glob
import shutil

DATA = "data"
DB_NAME = "myDB.db"
TABLE_NAME = "task6"
RES_FOLDER = "result"

def ger_csv_file(data):
    csv_files = glob.glob(os.path.join(data, "*.csv"))
    for csv_file in csv_files:
        return csv_file


def insert_into_duckdb(table_name, csv_path):
    conn = duckdb.connect(DB_NAME)    
    conn.execute(f"""
        CREATE TABLE {table_name} AS 
        SELECT * 
        FROM read_csv_auto('{csv_path}')
    """)
    print(conn.execute(f"SELECT * FROM {table_name} LIMIT 5;").fetchall())
    conn.close()

def calculate():
    conn = duckdb.connect(DB_NAME)
    if os.path.exists(RES_FOLDER):
        shutil.rmtree(RES_FOLDER)
    os.makedirs(RES_FOLDER)
    task1(conn)
    task2(conn)
    task3(conn)
    task4(conn)
    conn.close()

def task1(conn):
    result = conn.execute(f"""
        SELECT City, COUNT(*) as num_cars
        FROM {TABLE_NAME}
        GROUP BY City
    """).fetchdf()
    print("result of task1:\n", result)
    result.to_parquet(os.path.join(RES_FOLDER,'task1.parquet'))

def task2(conn):
    result = conn.execute(f"""
        SELECT Make, COUNT(*) as num_cars
        FROM {TABLE_NAME}
        GROUP BY Make
        ORDER BY num_cars DESC
        LIMIT 3
    """).fetchdf()
    print("result of task2:\n", result)
    result.to_parquet(os.path.join(RES_FOLDER,'task2.parquet'))

def task3(conn):
    result = conn.execute(f"""
        SELECT "Postal Code" as PostalCode, Make, COUNT(*) as num_cars
        FROM {TABLE_NAME}
        GROUP BY PostalCode, Make
        QUALIFY ROW_NUMBER() OVER(PARTITION BY PostalCode ORDER BY num_cars DESC) = 1
    """).fetchdf()
    print("result of task3:\n", result)
    result.to_parquet(os.path.join(RES_FOLDER,'task3.parquet'))

def task4(conn):
    result = conn.execute(f"""
        SELECT "Model Year" as Model_Year, COUNT(*) as num_cars
        FROM {TABLE_NAME}
        GROUP BY Model_Year
    """).fetchdf()
    print("result of task4:\n", result)
    result.to_parquet(os.path.join(RES_FOLDER,'task4.parquet'))


def main():
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
    insert_into_duckdb(TABLE_NAME, ger_csv_file(DATA))
    calculate()


if __name__ == "__main__":
    main()
