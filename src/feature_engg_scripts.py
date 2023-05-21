
import pandas as pd
from src.utils import read_from_s3

df_master = read_from_s3(filename = 'final_master_agg_file')
df_master.head()
# df_master = pd.read_csv("E:/E2E_DTP_Project/data/final_master_agg_file.csv")


def features_selection(data_s3: pd.DataFrame) -> pd.DataFrame:
    '''
    to get the unit price and category features fromsales data left join with the df_master
    Parameters
    ----------
    data_s3 : pd.DataFrame
        DESCRIPTION.

    Returns
    -------
    master_df_added_col : dataframe

    '''
    
    data_s3 = data_s3[['timestamp_stockagg', 'product_id_stockagg', 
                           'estimated_stock_pct','quantity_salesagg',
                            'timestamp_stocktemp', 'temp__sensor']]
    
    data_s3.rename(columns={
        'timestamp_stockagg': 'timestamp', 
        'product_id_stockagg': 'product_id',
        'estimated_stock_pct': 'estimated_stock_percent', 
        'quantity_salesagg': 'quantity', 
        'temp__sensor': 'sensor_temp'},
        inplace=True)
    
    df_sales = pd.read_csv('E:/E2E_DTP_Project/data/sales.csv')
    
    distinct_df_sales = df_sales.drop_duplicates(subset=['transaction_id', 'timestamp', 
                                                         'product_id', 'category',
                                                         'unit_price'])
    
    distinct_df_sales_subset = distinct_df_sales.drop_duplicates(subset= ['product_id', 'category','unit_price'])
    distinct_df_sales_subset = distinct_df_sales_subset[['product_id', 'category','unit_price']]
    
    master_df_added_col =  pd.merge(left= data_s3, 
                                    right=distinct_df_sales_subset,
                                    on=['product_id'], 
                                    how='left')
    
    master_df_added_col = master_df_added_col[['timestamp', 'product_id', 'estimated_stock_percent', 
                                               'quantity','sensor_temp', 'category', 'unit_price']]
    
    return master_df_added_col

feature_df = features_selection(data_s3 = df_master)
   
def feature_engg(features_selection_df : pd.DataFrame) -> pd.DataFrame:                                          
    '''
    It will use the dataframe from features_selection() function

    Parameters
    ----------
    features_selection_df : 

    Returns
    -------
    features_selection_df : TYPE
        DESCRIPTION.

    '''
    features_selection_df['timestamp'] = pd.to_datetime(features_selection_df['timestamp'])
    
    # Extract features from the timestamp column
    features_selection_df['day'] = features_selection_df['timestamp'].dt.day
    features_selection_df['dayofweek'] = features_selection_df['timestamp'].dt.dayofweek
    features_selection_df['year'] = features_selection_df['timestamp'].dt.year
    features_selection_df['month'] = features_selection_df['timestamp'].dt.month
    features_selection_df['hour'] = features_selection_df['timestamp'].dt.hour
    features_selection_df = pd.get_dummies(features_selection_df, columns=['category'], prefix='category')
    #features_selection_df get dummies
    
    return features_selection_df

def process() -> pd.DataFrame:
    '''
    Process will not take any kind of input parameter. This will help us to execute the process sequence wise
    First Feature secletion fucntion will run by passing the df_master dataframe and output of feature selection will 
    pass through the feature engg function and then outout of feature engg function will save the dataframe on the fly 
    on the amazon s3 bucket
    Returns
    -------
    feature_engg_final_df : final pandas dataframe which needs to upload on amazon s3 bucket

    '''
    feature_df = features_selection(data_s3 = df_master)
    feature_engg_final_df = feature_engg(feature_df)
    return feature_engg_final_df
    




