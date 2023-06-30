import argparse
import os
from pathlib import Path

import yaml

from utils import (connect_db, create_database, create_table_in_sql,
                   insert_data, python_df_to_sql_table, read_data)

parser = argparse.ArgumentParser(description='To create a data base in mysql and upload data into it')

parser.add_argument('-host', '--host_name', type=str, help = "pass the host_name to connect", required = True)  
parser.add_argument('-user', '--user_name', type=str, help = "pass the user_name to connect", required = True) 
parser.add_argument('-db', '--create_db' , default = False ,type=bool, help="Create database or not", required= False) 
parser.add_argument('-dbname', '--database_name', type=str, help = "pass the databse_name to connect", required = True)
parser.add_argument('-id', '--task_id' , type=str, help="The tasks defined in the config files.", required= False) 

args = parser.parse_args()

# load tasks from config --------------------------------------------------------------
with open("./config/config.yaml", 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# step 1: connect to database
conn, cur = connect_db(host= args.host_name, user= args.user_name)

# step 2: read dataset
if args.create_db:
     create_database(database_name= args.database_name, cur= cur)
else:
    # define variables
    #args_task_id = 'upload-to-database'
    config_import = config[args.task_id]["import"]
    for i in range(len(config_import)):
        data = Path(config_import[i]["import"]["dirpath"],
                    config_import[i]["import"]["prefix_filename"] + '.' +
                    config_import[i]["import"]["file_extension"])
        table_name = os.path.basename(data).split('.')[0]
        print(table_name)
        df = read_data(filename= data)
        print(df.columns)
        coltype, values = python_df_to_sql_table(df)
        create_table_in_sql(database_name= args.database_name, table_name=  table_name, coltype=coltype, cur=cur)
        insert_data(dataframe=df,  table_name= table_name, values=values, cur=cur, conn=conn)
