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

env = Path("src/.env")
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
cur.close()
conn.close()





# # STEP 2 CREATE A FUNCTION: GET_DATA OR READ_DATA
# Filename argurment
def read_data(filename: str) -> pd.DataFrame:
    '''
    Reads a CSV file and returns a pandas DataFrame.

    Parameters
    ----------
    filename : str
    - file_path (str): The path to the CSV file.   

    Returns
    -------
    df : TYPE
        DESCRIPTION.
    - pandas.DataFrame: A DataFrame containing the data from the CSV file.

    '''
    df = pd.read_csv(filename)
    return df


# # STEP 3 CREATE FUNCTION CREATE DATABASE - RETURNS DATABASES
# database name 
def create_database(database_name: str): 
    '''
    This function will create the database and will return list of all databases
    
    Parameters
    ----------
    database_name : str
        Pass the Database name which we want to create it

    Returns
    -------
    databases : TYPE
        DESCRIPTION.

    '''
    conn, cur = connect_db(host="localhost", user="root")
    cur.execute(f"CREATE DATABASE {database_name}")
    cur.execute("SHOW DATABASES")
    databases = cur.fetchall()
    cur.close()
    conn.close()
    return databases

# # STEP 4
def python_df_to_sql_table(dataframe: str) ->  pd.DataFrame :
    '''
    This function will used to create the  table structure from Python dataframe to SQL table
    coltype will return 'colname datatype'
    values will return the place holder to inster the data into it and it will have number of count will 
    equal to no of columns
    
    Parameters
    ----------
    dataframe : str
        DESCRIPTION.

    Returns
    -------
    coltypes : TYPE
        output will be one string with colname and dattype 'colA INT, colb FLOAT'
    values : TYPE
        Placeholder return. No of columns will equal no of place holder 

    '''
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

coltype, values = python_df_to_sql_table(dataframe)


# # STEP 5 CREATE FUNCTION CREATE_TABLE - NO RETURN
def create_table_in_sql(database_name: str, table_name: str, dataframe: str):
    '''
    This function will create the datbase
     
    Parameters
    ----------
    database_name : str
        Pass the database name which is already created here new database will not get create
    table_name : str
       Pass the table name which we need to create the table inside the database

    Returns
    -------
    It will create table inside the database

    '''
    conn, cur = connect_db(host="localhost", user="root")
    
    coltype, values = python_df_to_sql_table(dataframe)
    
    cur.execute(f"use {database_name}")
    cur.execute(f"CREATE TABLE {table_name} ({coltype}")
    cur.execute("SHOW TABLES")
    tables = cur.fetchall()
    cur.close()
    conn.close()
    return tables


   


# Step 6

def python_to_sql_data_transfer_func(dataframe: str, table_name: str):
    '''
    Passing  the data from python datframe to sql table

    Parameters
    ----------
    dataframe : str
        passing the python dataframe which is read from the system 
    table_name : str
        where we need to store the python dataframe data to sql table

    Returns
    -------
    None.

    '''
    conn, cur = connect_db(host="localhost", user="root")
    coltype, values = python_df_to_sql_table(dataframe)
    
    for i, row in dataframe.iterrows():    
        sql = f"INSERT INTO {table_name} VALUES ({values})"
        cur.execute(sql, tuple(row))
        conn.commit()
        
    cur.execute(f"SELECT * FROM {table_name}")
    myresult = cur.fetchall()
    for x in myresult:
      print(x)
    cur.close()
    conn.close()