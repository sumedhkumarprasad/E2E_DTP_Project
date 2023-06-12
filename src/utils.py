# -*- coding: utf-8 -*-
"""
Created on Sun Apr 16 19:12:51 2023

@author: Sumedh Prasad

utils code
"""

import logging
import importlib
import os
from pathlib import Path
from typing import List, Tuple

import mysql.connector as mysql
import pandas as pd
from dotenv import load_dotenv
import sys
import boto3
from io import StringIO
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd

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
    # Load data from CSV file
    df = pd.read_csv(filename)
    # Drop the 'Unnamed: 0' column, if it exists
    df.drop(columns = ['Unnamed: 0'], inplace = True, errors = 'ignore')
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
        

        
def authenticate_s3() -> boto3.client:
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
    except Exception as e:
        logging.debug(e)
    
    # Return the S3 client and bucket name as a tuple
    return client

def upload_to_s3(df: pd.DataFrame, filename: str, which_bucket: str) -> bool:
    '''
    This function will first call the authenticate_s3() and then establish the S3 connection with local Python.
    
    Parameters
    ----------
    df : pd.DataFrame
        Final master dataset to be uploaded to the S3 bucket.
    filename : str
        Name of the file to be saved.
    which_bucket : str
        Name of the S3 bucket to upload the file to.

    Returns
    -------
    bool
        True if the upload is successful, False otherwise.
    '''
    # Authenticate with AWS
    client = authenticate_s3()

    # Upload the file
    try:
        logging.info('Uploading final dataframe to Amazon S3 bucket...')
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        response = client.put_object(
            ACL='private',
            Body=csv_buffer.getvalue(),
            Bucket=which_bucket,
            Key=filename + '.csv'
        )
        logging.info('Successfully uploaded final dataframe to Amazon S3 bucket.')
        return True
    except Exception as e:
        logging.error('Failed to upload to S3 bucket: ' + str(e))
        return False

def read_from_s3(filename: str, which_bucket: str) -> pd.DataFrame:
    
    '''
    Reading the file from s3 bucket

    Parameters
    ----------
    filename : pass the filename of s3 bucket

    Returns : pandas dataframe

    '''
    # Authenticate with AWS
    client = authenticate_s3()

    # Upload the file
    try:
        response = client.get_object(Bucket = which_bucket, Key=filename + '.csv')
        read_df = pd.read_csv(response['Body'])
    except Exception as e:
        raise Exception(f"Error reading file {filename} from S3: {str(e)}")
    return read_df 

def upload_to_googlesheet(df: pd.DataFrame, g_excel_sheet_id: str, worksheet_name: str, filename: str) -> bool:
    '''
    This function is used to upload the python dataframe into the google sheet
 
    Parameters
    ----------
    g_excel_sheet_id : str
        Pass the google sheet id after creating the google sheet.
    df : pd.DataFrame
        python data frame which need to pass the python dataframe to the googlesheet.
    worksheet_name : str
        Pass the worksheet name by which new worksheet needs to be created

    Returns
    -------
    bool
        successfully updated or not in the google sheet.

    '''
    SCOPES = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
    ]
    
    load_dotenv('.env')
    
    typee = os.getenv("TYPE")
    project_id = os.getenv("PROJECT_ID")
    private_key_id = os.getenv("PRIVATE_KEY_ID")
    private_key = os.getenv("PRIVATE_KEY").replace("\\n", "\n")
    client_email = os.getenv("CLIENT_EMAIL")
    client_id = os.getenv("CLIENT_ID")
    auth_uri = os.getenv("AUTH_URI")
    token_uri = os.getenv("TOKEN_URI")
    auth_provider_x509_cert_url = os.getenv("AUTH_PROVIDER_X509_CERT_URL")
    client_x509_cert_url = os.getenv("CLIENT_X509_CERT_URL")
    universe_domain = os.getenv("UNIVERSE_DOMAIN")

    credentials = ServiceAccountCredentials.from_json_keyfile_dict({
        "type": typee,
        "project_id": project_id,
        "private_key_id": private_key_id,
        "private_key": private_key,
        "client_email": client_email,
        "client_id": client_id,
        "auth_uri": auth_uri,
        "token_uri": token_uri,
        "auth_provider_x509_cert_url": auth_provider_x509_cert_url,
        "client_x509_cert_url": client_x509_cert_url,
        "universe_domain": universe_domain
    }, scopes=SCOPES)

    google_auth = gspread.authorize(credentials)

    try:
        spreadsheet = google_auth.open_by_key(g_excel_sheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1, cols=1)
    
     # Check if the worksheet already exists with the provided name
    if worksheet.title == worksheet_name:
        # Clear the existing worksheet
        worksheet.clear()
    else:
        # Rename the worksheet to the provided filename
        worksheet.update_title(filename)
    
    df = df.astype(str)
    cell_list = worksheet.update([df.columns.values.tolist()] + df.values.tolist())

    if cell_list:
        return True
    else:
        return False

        
def process_task(task: str) -> dict:
    """Import task file to process the data from src/tools folder
    Args:
        task (str): name of the task to process
    Returns:
        dict: processed_data
    """
    
    lib = importlib.import_module(f"src.{task}")
    return lib.process()   
    

