# -*- coding: utf-8 -*-
"""
Created on Sun Apr 16 19:12:51 2023

@author: Sumedh Prasad

utils code
"""

import logging
import os
from pathlib import Path
from typing import List, Tuple

import mysql.connector as mysql
import pandas as pd
from dotenv import load_dotenv
import sys
import boto3
from io import StringIO

sys.dont_write_bytecode = True


logging.basicConfig(filename='myapp.log',
                    format='%(asctime)s %(message)s', 
                    level=logging.INFO)


# STEP 1
def connect_db(host: str, user: str) -> Tuple[mysql.connection.MySQLConnection, str]:
    """this function connects to mysql database and returns conn, cur as tuple

    Args:
        host (str): pass host name
        user (str): pass user name

    Returns:
        Tuple[mysql.connection.MySQLConnection, str]: 
    """
    env = Path(".env")
    load_dotenv(dotenv_path=env)
    try:
        logging.info('Started MySQl Connection Started')
        conn = mysql.connect(host=host,
                            user=user,
                            password=os.getenv("PASSWORD"))
        cur = conn.cursor()
        logging.info("Connected to MySQL successfully!")
        logging.info('Ended MySQl Connection Started')
    except Exception as e:
        logging.debug(e)
    return conn, cur
    

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
    try:
        logging.info('Started  reading csv format file')
        df = pd.read_csv(filename)
        df.drop(columns = ['Unnamed: 0'], inplace = True, errors = 'ignore')
        logging.info("Read CSV File successfully!")
        logging.info('Ended reading csv format file')
    except Exception as e:
        logging.debug(e)
    return df
        
    


# # STEP 3 CREATE FUNCTION CREATE DATABASE - RETURNS DATABASES
# database name 
def create_database(database_name: str, cur: str): 
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
    try:
        logging.info('Started  creating database')
        cur.execute(f'DROP DATABASE IF EXISTS {database_name}')
        cur.execute(f"CREATE DATABASE {database_name}")
        cur.execute("SHOW DATABASES")
        databases = cur.fetchall()
        logging.info("Created Databse successfully!")
        logging.info('Ended creating database')
        print(databases)
    except Exception as e:
         logging.debug(e)
         
    


# # STEP 4
def python_df_to_sql_table(dataframe: pd.DataFrame) -> Tuple[str, str]:
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
    try:
        logging.info('Started  python to sql data type conversion')
        types =[]
        for type in dataframe.dtypes:
            if type == 'object':
                types.append('VARCHAR(255)')
            elif type == 'float64':
                types.append('FLOAT')
            elif type == 'int64':
                types.append('INT')
    
        coltypes = list(zip(dataframe.columns.values, types))
        coltypes = tuple([" ".join(i) for i in coltypes])
        coltypes = ", ".join(coltypes)
        values = ', '.join(['%s' for _ in range(len(dataframe.columns))])
        logging.info("Created tuple of column and datatype for creating table in SQl")
        logging.info('Ended  python to sql data type conversion')
        
    except Exception as e:
         logging.debug(e)
    
    return coltypes, values


# # STEP 5 CREATE FUNCTION CREATE_TABLE - NO RETURN
def create_table_in_sql(database_name: str, table_name: str, coltype: str, cur: mysql.cursor.MySQLCursor):
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
    try:
        logging.info('Started creating table in the databse')
        cur.execute(f"USE {database_name}")
        cur.execute(f'DROP TABLE IF EXISTS {table_name}')
        cur.execute(f"CREATE TABLE {table_name} ({coltype})")
        cur.execute("SHOW TABLES")
        tables = cur.fetchall()
        print(tables)
        logging.info('Created table in the databse')
        logging.info('Ended creating table in the databse')
    except Exception as e:
         logging.debug(e)
         

# Step 6
def insert_data(dataframe: str, table_name: str, values: str, cur: str, conn: str):
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
    try:
        logging.info('Started putting python dataframe data into mysql db data')
        # cur.execute(f"USE {database_name}")
        for _, row in dataframe.iterrows(): 
            sql = f"INSERT INTO {table_name} VALUES ({values})"
            cur.execute(sql, tuple(row))
            conn.commit()
        logging.info('Putted the data into SQl table successfully')
        logging.info('Ended putting python dataframe data into mysql db data')
        
        cur.execute(f"SELECT * FROM {table_name}")
        myresult = cur.fetchall()
        print(len(myresult))
    except Exception as e:
        logging.debug(e)
        

        
def authenticate_s3() -> Tuple[boto3.client, str]:
    '''
    This function will use the boto3 python library and establish aws s3 bcuket connection 
    Returns -- No return
    No Parameter pass
    '''
    env = Path(".env")
    load_dotenv(dotenv_path=env)
    try:
        logging.info('establish and authenticate_s3 through local python')
        client = boto3.client('s3', aws_access_key_id=os.getenv('access_key_id'), 
                          aws_secret_access_key=os.getenv('secret_access_key'), 
                          region_name=os.getenv('region'))
        logging.info('connection established and authenticate_s3 through local python successfully')
        logging.info('Ended establish and authenticate_s3 through local python')    
        bucket_name = os.getenv('bucket_name')
    except Exception as e:
        logging.debug(e)
    
    # Return the S3 client and bucket name as a tuple
    return client, bucket_name

def upload_to_s3(df: pd.DataFrame, filename: str) -> bool:
    '''
    This function will first call the authenticate_s3() an then establsih the s3 connection with local python
    
    Parameters
    ----------
    df : final master dataset which needs to upload on s3 bucket
    filename : give the file name which you want to save it.

    Returns
    -------
    bool
        DESCRIPTION.

    '''
    # Authenticate with AWS
    client, bucket_name = authenticate_s3()

    # Upload the file
    try:
        logging.info('Upload final dataframe to amaxzon s3 bucket starting ')
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        client.put_object(
           # ACL = 'private',
            Body= csv_buffer.getvalue(),
            Bucket = bucket_name,
            Key = filename + '.csv'
        )
        
    except Exception as e:
        print(e)
        return logging.info('Not successfully uploaded to s3 bucket ')
    return logging.info('successfully uploaded final dataframe to amazon s3 bucket starting ')

def read_from_s3(filename: str) -> pd.DataFrame:
    
    '''
    Reading the file from s3 bucket

    Parameters
    ----------
    filename : pass the filename of s3 bucket

    Returns : pandas dataframe

    '''
    # Authenticate with AWS
    client, bucket_name = authenticate_s3()

    # Upload the file
    try:
        response = client.get_object(
            Bucket = bucket_name,
            Key=filename + '.csv')
        
        read_df = pd.read_csv(response['Body'])
    except Exception as e:
        print(e)
        return False
    return read_df 

        
   
    

