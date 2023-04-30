from pathlib import Path

from dotenv import load_dotenv

from utils import (connect_db, create_database, create_table_in_sql,
                       python_df_to_sql_table, read_data,
                       insert_data)
import argparse

parser = argparse.ArgumentParser(description='To create a data base in mysql and upload data into it')

parser.add_argument('-host', '--host_name', type=str, help = "pass the host_name to connect", required = True)  
parser.add_argument('-user', '--user_name', type=str, help = "pass the user_name to connect", required = True) 
parser.add_argument('-db', '--create_db' , default = False ,type=bool, help="Create database or not", required= False) 
parser.add_argument('-dbname', '--database_name', type=str, help = "pass the databse_name to connect", required = True)
parser.add_argument('-f', '--file_path' , type=str, help="pass the input dataset", required= False) 


args = parser.parse_args()

env = Path("src/.env")
load_dotenv(dotenv_path=env)

# step 1: connect to database
conn, cur = connect_db(host= args.host_name, user= args.user_name)

# step 2: read dataset
# df = read_data(filename= parser['file_path'])



# step 2: either create database or use existing database
# database = True

if args.create_db:
     create_database(database_name= args.dbname , cur= cur)
   
else:
    df = read_data(filename= args.file_path)
    df.columns
    coltype, values = python_df_to_sql_table(df)
    create_table_in_sql(database_name= args.dbname, table_name=  args.tname, coltype=coltype, cur=cur)
    insert_data(dataframe=df, table_name= args.tname, values=values, cur=cur, conn=conn)
    
