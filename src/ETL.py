# -*- coding: utf-8 -*-
"""
Created on Mon May  1 10:29:39 2023

@author: Sumedh Prasad
"""

import logging
import os
from pathlib import Path
from typing import List, Tuple
from datetime import datetime

import mysql.connector as mysql
import pandas as pd
from dotenv import load_dotenv
import sys

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
    df.drop(columns = ['Unnamed: 0'], inplace = True, errors = 'ignore')
       
    return df

def convert_to_datetime(data: pd.DataFrame = None, column: str = None):

  dummy = data.copy()
  dummy[column] = pd.to_datetime(dummy[column], format='%Y-%m-%d %H:%M:%S')
  return dummy



def convert_timestamp_to_hourly(data: pd.DataFrame = None, column: str = None):
  dummy = data.copy()
  new_ts = dummy[column].tolist()
  new_ts = [i.strftime('%Y-%m-%d %H:00:00') for i in new_ts]
  new_ts = [datetime.strptime(i, '%Y-%m-%d %H:00:00') for i in new_ts]
  dummy[column] = new_ts
  return dummy


def df_agg(data: pd.DataFrame = None, groupby_col: list = None, agg_col: str = None, agg_func: str = None):
    
    dataframe_agg = data.groupby(groupby_col)[agg_col].agg(agg_func).reset_index()
    
    return dataframe_agg


df_sales = read_data("E:/E2E_DTP_Project/data/sales.csv")
df_sales = convert_to_datetime(data = df_sales, column = 'timestamp')
df_sales = convert_timestamp_to_hourly(data = df_sales, column = 'timestamp' )
sale_agg =  df_agg(data= df_sales , groupby_col= ['timestamp','product_id'], agg_col= 'quantity', agg_func ='sum')


df_stock_levels = read_data("E:/E2E_DTP_Project/data/sensor_stock_levels.csv")
df_stock_levels = convert_to_datetime(data = df_stock_levels, column = 'timestamp')
df_stock_levels = convert_timestamp_to_hourly(data = df_stock_levels, column = 'timestamp' )
stock_level_agg =  df_agg(data= df_stock_levels , groupby_col= ['timestamp','product_id'], agg_col= 'estimated_stock_pct', agg_func ='mean')
                      

df_stock_temp = read_data("E:/E2E_DTP_Project/data/sensor_storage_temperature.csv")
df_stock_temp = convert_to_datetime(data = df_stock_temp, column = 'timestamp')
df_stock_temp = convert_timestamp_to_hourly(data = df_stock_temp, column = 'timestamp' )
df_stock_temp_agg =  df_agg(data= df_stock_temp , groupby_col= ['timestamp'], agg_col= 'temperature', agg_func ='mean')
  
