# -*- coding: utf-8 -*-
"""
Created on Sat May  6 14:35:34 2023

@author: Sumedh Prasad
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



from src.utils import (connect_db,authenticate_s3,upload_to_s3,read_from_s3)


def execute_sql_script(sql_script_file_path: str) -> pd.DataFrame:
    '''
    This function is used for executing the SQl script from saved sql scripts

    Parameters
    ----------
    sql_script_file_path : str
        DESCRIPTION.

    Returns : Pandas dataframe will return
    -------

    '''
    conn, cur = connect_db(host = "localhost", user = "root")
    # Read the SQL script from file
    with open(sql_script_file_path, 'r') as file:
        sql_script = file.read()

    # Establish a connection to the MySQL database
    
    cur.execute(sql_script)

    # Fetch the result as a Pandas DataFrame
    result = cur.fetchall()
    df = pd.DataFrame(result, columns=cur.column_names)
    
    
    # Close the cursor and connection
    cur.close()
    conn.close()

    # Return the result as a Pandas DataFrame
    return df

def process() -> pd.DataFrame:
    sql_path_file_path = "src/final_master_agg_cleaned_data.sql"
    master_df = execute_sql_script(sql_script_file_path = sql_path_file_path)
    return upload_to_s3(df = master_df, filename = "sample_123456")


