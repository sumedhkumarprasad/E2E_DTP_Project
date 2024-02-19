
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

import pytest

sys.dont_write_bytecode = True

from utils import (connect_db,read_data,authenticate_s3)



HOST = 'localhost'
USER = 'root'

def test_connect_db():
    conn, cur = connect_db(host = HOST, user = USER)
    assert conn.is_connected()
   

def test_read_data():
    data = {
        'Column1': [1, 2, 3],
        'Column2': ['A', 'B', 'C']
    }
    df_sample = pd.DataFrame(data)
    df_sample.to_csv('data/sample_data.csv', index = False, header= True)
    df_read_data= read_data('data/sample_data.csv')
    assert not df_read_data.empty

def test_authenticate_s3():
    var_client = authenticate_s3()
    assert var_client  is not None


