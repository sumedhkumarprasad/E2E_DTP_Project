
import pandas as  pd
import numpy as np


from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import SelectFromModel
from sklearn.metrics import mean_squared_error, r2_score,mean_absolute_error
from sklearn.model_selection import GridSearchCV
from sklearn.preprocessing import StandardScaler
from src.utils import (read_from_s3,read_data,upload_to_s3,authenticate_s3,upload_to_googlesheet)


df = read_from_s3(filename = "final_feature_engg_master" , which_bucket = "processed_data_bucket_name" )

def missing_value_treatment(input_df: pd.DataFrame)  -> pd.DataFrame:  
    

    null_rows = input_df[input_df.isnull().any(axis=1)]
    print("Rows with null, NaN, or None values:")
    print(null_rows)

    null_columns = input_df.columns[input_df.isnull().any()]
    print("\nColumns with null, NaN, or None values:")
    print(null_columns)

    input_df['quantity'] =input_df['quantity'].fillna(input_df['quantity'].mean())
    input_df = input_df.sort_values(by = "timestamp")
    
    input_df.reset_index(inplace=True)
    input_df.drop('index', axis=1, inplace=True)
    
    input_df.set_index('timestamp',inplace = True)
    
    return input_df

# treated_df11 = missing_value_treatment(input_df=df)


    
def model_train_func(treated_df : pd.DataFrame)  -> pd.DataFrame:
    
    cut_off_date = "2022-03-06 10:00:00"
    
    X_train, X_test = treated_df[:cut_off_date],treated_df[cut_off_date:]

    X_train.reset_index(inplace=True)
    X_test.reset_index(inplace=True)
    
    
    y_train = pd.DataFrame(X_train['estimated_stock_percent'])
    y_test =  pd.DataFrame(X_test['estimated_stock_percent'])
     
    X_train.drop('estimated_stock_percent', axis=1, inplace=True)
    X_test.drop('estimated_stock_percent', axis=1, inplace=True)
     
     
    x_test_columns = X_test.columns
    X_test_df = pd.DataFrame(X_test.copy(), columns=x_test_columns)
     
    x_train_columns = X_train.copy().columns
    X_train_df = pd.DataFrame(X_train.copy(), columns = x_train_columns)
    X_train_df['estimated_stock_percent'] = y_train
    
    X_train.drop('timestamp', axis=1, inplace=True)
    X_test.drop('timestamp', axis=1, inplace=True)
     
    yy_train = y_train.copy().to_numpy().ravel() 
    regressor = RandomForestRegressor()
    scaler = StandardScaler()
    scaler.fit(X_train)
    X_train = scaler.transform(X_train)
    X_test = scaler.transform(X_test)
     
    model_train= regressor.fit(X_train,yy_train)
    
    return X_train, X_test, y_train, y_test, model_train ,X_test_df,X_train_df



def model_metrics(read_inital_data_df: pd.DataFrame)-> pd.DataFrame :
    treated_missing_df = missing_value_treatment(input_df= read_inital_data_df)
    X_train, X_test, y_train, y_test, model_train ,X_test_df,X_train_df  = model_train_func(treated_df = treated_missing_df)
    y_pred = model_train.predict(X_test)
    
    feature_importances = model_train.feature_importances_
    
    X_test_df_columns = list(X_test_df.columns)
    X_test_df_columns.remove('timestamp')
    
    importance_df = pd.DataFrame({'Feature': X_test_df_columns, 'Importance': feature_importances})
    importance_df = importance_df.sort_values(by='Importance',ascending=False)
    X_test_df['y_pred'] = y_pred 
    
    mse = mean_squared_error(y_test, y_pred)
    mae = mean_absolute_error(y_test,y_pred)
    r2 = r2_score(y_test, y_pred)
    rmse = np.sqrt(mse)

    print(f"Mean Squared Error: {mse:.2f}")
    print(f"MAE : {mae:.2f}")
    print(f"RMSE: {rmse:.2f}")
    print(f"R-squared: {r2:.2f}")
    
    metrics_data  = {
    'Metric': ['Mean Squared Error', 'MAE', 'RMSE', 'R-squared'],
    'Value': [mse, mae, rmse, r2]
     }
    
    metrics_df = pd.DataFrame(metrics_data)
    
    return X_train_df,X_test_df,importance_df,metrics_df



######################################################################
def process():
        
    historical_df,final_prediction_df, feature_imp_df,metrics_df =  model_metrics(read_inital_data_df = df )
    
    category_col = final_prediction_df.columns[final_prediction_df.columns.str.startswith('category')]
    category_col_split_series_test_dataset = final_prediction_df[category_col].idxmax(axis=1).str.split('_', expand=True)[1]
    final_prediction_df.drop(category_col, axis=1, inplace=True)
    final_prediction_df['category'] = category_col_split_series_test_dataset
    final_prediction_df = final_prediction_df.rename(columns={'y_pred': 'future_estimated_stock_percent'})
    
    
    historical_category_col = historical_df.columns[historical_df.columns.str.startswith('category')]
    historical_category_col_split_series_test_dataset = historical_df[category_col].idxmax(axis=1).str.split('_', expand=True)[1]
    historical_df.drop(historical_category_col, axis=1, inplace=True)
    historical_df['category'] = historical_category_col_split_series_test_dataset
    historical_df = historical_df.rename(columns={'estimated_stock_percent': 'historical_estimated_stock_percent'})
    
    
    upload_to_googlesheet(g_excel_sheet_id = "1eqjRxxBI45A7CqjQ_D3UQCRTJyI7hujh4_Q3kXFeidA", df = historical_df  , worksheet_name = "Historical_Data")
    upload_to_googlesheet(g_excel_sheet_id = "1eqjRxxBI45A7CqjQ_D3UQCRTJyI7hujh4_Q3kXFeidA", df = final_prediction_df  , worksheet_name = "Future_Data")
    upload_to_googlesheet(g_excel_sheet_id = "1eqjRxxBI45A7CqjQ_D3UQCRTJyI7hujh4_Q3kXFeidA", df = feature_imp_df  , worksheet_name = "Feature_Importance")
    upload_to_googlesheet(g_excel_sheet_id = "1eqjRxxBI45A7CqjQ_D3UQCRTJyI7hujh4_Q3kXFeidA", df = metrics_df  , worksheet_name = "Model_Metrics")
    
process()

##################################################################################

# final dataset = x_test+ y_test+y_pred
#  product information 
############################################################################################

# # Calculate the correlation between features and target variable
# correlation_values = X.apply(lambda feature: np.abs(np.corrcoef(feature, y)[0, 1]))
# correlation_df = pd.DataFrame({'Feature': X.columns, 'Correlation': correlation_values})

# # Sort the features by correlation in descending order
# correlation_df.sort_values(by='Correlation', ascending=False, inplace=True)


# threshold = 0.01  # Adjust the threshold as per your preference
# selected_features = correlation_df.loc[correlation_df['Correlation'] > threshold, 'Feature'].tolist()

# # Subset the data with selected features
# X_selected = X[selected_features]

# # Split the data into train and test sets
# X_train, X_test, y_train, y_test = train_test_split(X_selected, y, test_size=0.3, random_state=42)

# # Train the random forest regressor
# regressor = RandomForestRegressor()
# regressor.fit(X_train, y_train)

# # Model evaluation
# y_pred = regressor.predict(X_test)

# mse = mean_squared_error(y_test, y_pred)
# r2 = r2_score(y_test, y_pred)
# rmse = np.sqrt(mse)

# print(f"Mean Squared Error: {mse:.2f}")
# print(f"RMSE: {rmse:.2f}")
# print(f"R-squared: {r2:.2f}")

##########################################################################################


# Split the data into train and test sets
# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)



# feature_selector = SelectFromModel(RandomForestRegressor(n_estimators=100))
# X_train_selected = feature_selector.fit_transform(X_train, y_train)
# X_test_selected = feature_selector.transform(X_test)

# # Hyperparameter tuning using GridSearchCV
# param_grid = {
#     'n_estimators': [100, 200, 300],
#     'max_depth': [None, 5, 10],
#     'min_samples_split': [2, 5, 10]
# }
# grid_search = GridSearchCV(RandomForestRegressor(), param_grid, cv=5)
# grid_search.fit(X_train_selected, y_train)
# best_model = grid_search.best_estimator_

###########################################################################################


# ##### Base Model #####
# regressor = RandomForestRegressor()
# scaler = StandardScaler()
# scaler.fit(X_train)
# X_train = scaler.transform(X_train)
# X_test = scaler.transform(X_test)
# model_train= regressor.fit(X_train,y_train)



# Model evaluation
# y_pred = model_train.predict(X_test)
# mse = mean_squared_error(y_test, y_pred)
# mae = mean_absolute_error(y_test,y_pred)
# r2 = r2_score(y_test, y_pred)
# rmse = np.sqrt(mse)

# print(f"Mean Squared Error: {mse:.2f}")
# print(f"MAE : {mae:.2f}")
# print(f"RMSE: {rmse:.2f}")
# print(f"R-squared: {r2:.2f}")