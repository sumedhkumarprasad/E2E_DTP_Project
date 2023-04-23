from pathlib import Path

from dotenv import load_dotenv

from src.utils import (connect_db, create_database, create_table_in_sql,
                       python_df_to_sql_table, read_data,
                       insert_data)

env = Path("src/.env")
load_dotenv(dotenv_path=env)

# step 1: connect to database
conn, cur = connect_db(host= "localhost", user= "root")

# step 2: read dataset
df = read_data(filename="data/sales.csv")
df.columns
# step 2: either create database or use existing database
database = True

if database:
    databases = create_database(database_name= "raw_data", cur= cur)
    print(databases)
else:
    coltype, values = python_df_to_sql_table(df)
    create_table_in_sql(database_name= "raw_data", table_name= "sales", coltype=coltype, cur=cur)
    insert_data(dataframe=df, table_name="sales", values=values, cur=cur, conn=conn)
    
