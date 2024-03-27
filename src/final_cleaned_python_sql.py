# -*- coding: utf-8 -*-
"""
Created on Sat May  6 14:35:34 2023

@author: Sumedh Prasad
"""

import mysql.connector as mysql
import pandas as pd
from pathlib import Path

from src.utils import (connect_db)


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
    '''
    This function will run the process step by step, does not take any input parameter 
    first take the sql scripts file then run or execute the SQL scripts through function and output of that sql scripts 
    needs to upload to amazon s3 bucket on the fly without sving the data into disk

    Returns - Final pandas dataframe which will upload on the amazon s3 bucket
    '''
    sql_path_file_path = Path("src/final_master_agg_cleaned_data.sql")
    master_df = execute_sql_script(sql_script_file_path = sql_path_file_path)
    return master_df


