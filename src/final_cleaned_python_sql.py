# -*- coding: utf-8 -*-
"""
Created on Sat May  6 14:35:34 2023

@author: Sumedh Prasad
"""

import logging
import os
from pathlib import Path
from typing import List, Tuple
import sqlite3
import mysql.connector as mysql
import pandas as pd
from dotenv import load_dotenv
import sys

env = Path("src/.env")
load_dotenv(dotenv_path=env)

def connect_db(host: str, user: str) -> Tuple[mysql.connection.MySQLConnection, str]:
    """this function connects to mysql database and returns conn, cur as tuple

    Args:
        host (str): pass host name
        user (str): pass user name

    Returns:
        Tuple[mysql.connection.MySQLConnection, str]: 
    """
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



sql_path = "E:/E2E_DTP_Project/final_master_agg_cleaned_data.sql"


def execute_sql_script( file_path: str):
    conn, cur = connect_db(host = "localhost", user = "root")
    # Read the SQL script from file
    with open(file_path, 'r') as file:
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

master_df = execute_sql_script( file_path = sql_path)
master_df.head()
