# -*- coding: utf-8 -*-
"""
Created on Sun Apr 16 19:12:51 2023

@author: Sumedh Prasad

utils code
"""

from typing import List, Tuple
import os
from os.path import join, dirname
import logging

from pathlib import Path
import mysql.connector as mysql
from dotenv import load_dotenv

import pandas as pd
import numpy as np

# os.chdir(Path("E:/E2E_DTP_Project"))
#Path("E:/E2E_DTP_Project")

logging.basicConfig(filename='myapp.log',
                    format='%(asctime)s %(message)s', 
                    level=logging.INFO)

env = Path(".env.txt")
load_dotenv(dotenv_path=env)

# STEP 1
def connect_db(host: str, user: str) -> Tuple[mysql.connection.MySQLConnection, str]:
    """this function connects to mysql database and returns conn, cur as tuple

    Args:
        host (str): pass host name
        user (str): pass user name

    Returns:
        Tuple[mysql.connection.MySQLConnection, str]: 
    """
    try:
        logging.info('Started')
        conn = mysql.connect(host=host,
                            user=user,
                            password=os.getenv("PASSWORD"))
        cur = conn.cursor()
        logging.info("Connected to MySQL successfully!")
        logging.info('ended')
    except Exception as e:
        logging.debug(e)
    return conn, cur

conn, cur = connect_db(host="localhost", user="root")



# STEP 2 CREATE A FUNCTION: GET_DATA OR READ_DATA
sample_data_df = pd.read_excel( 'sample_data_mysql.xlsx', sheet_name= "sample_data" )
sample_data_df.head(5)

# STEP 3 CREATE FUNCTION CREATE DATABASE - RETURNS DATABASES
cur.execute("CREATE DATABASE sample_data_upload")
cur.execute("SHOW DATABASES")
databases = cur.fetchall()

# STEP 4
def python_df_to_sql_table(dataframe):
    types =[]
    for type in dataframe.dtypes:
        if type == "object":
            types.append("VARCHAR(255)")
        elif type == "float64":
            types.append("FLOAT")
        elif type == 'int64':
            types.append("INT")

    coltypes = list(zip(dataframe.columns, types))
    coltypes = tuple([' '.join(i) for i in coltypes])
    coltypes = ', '.join(coltypes)
    values = ', '.join(["%s" for i in range(len(dataframe.columns))])
    return coltypes, values

coltype, values = python_df_to_sql_table(sample_data_df)

# STEP 5 CREATE FUNCTION CREATE_TABLE - NO RETURN
cur.execute("use sample_data_upload")
cur.execute("CREATE TABLE sample_data_table (sl_no int,cust_id  VARCHAR(255) PRIMARY KEY, region VARCHAR(255), date DATE)")



   


# for step 6

for i, row in sample_data_df.iterrows():    
    sql = f"INSERT INTO sample_data_table VALUES ({values})"
    cur.execute(sql, tuple(row))
    conn.commit()
cur.close()
conn.close()
    
cur.execute("SELECT * FROM sample_data_table")
myresult = cur.fetchall()
myresult 

for x in myresult:
  print(x)