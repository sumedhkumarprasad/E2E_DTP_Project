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
parser.add_argument('-tname', '--table_name' , type=str, help="pass the input table name", required= False) 

args = parser.parse_args()

env = Path("src/.env")
load_dotenv(dotenv_path=env)

# step 1: connect to database
conn, cur = connect_db(host= args.host_name, user= args.user_name)

# step 2: read dataset

if args.create_db:
     create_database(database_name= args.database_name, cur= cur)
   
else:
    df = read_data(filename= args.file_path)
    print(df.columns)
    coltype, values = python_df_to_sql_table(df)
    create_table_in_sql(database_name= args.database_name, table_name=  args.table_name, coltype=coltype, cur=cur)
    insert_data(dataframe=df,  table_name= args.table_name, values=values, cur=cur, conn=conn)
    
# args.file_path = "E:/E2E_DTP_Project/data/sales.csv"
# args.host_name = "localhost"
# args.user_name = "root"
# args.database_name = "raw_data"
# args.table_name = "sales"