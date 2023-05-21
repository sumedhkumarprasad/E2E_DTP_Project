
import pandas as  pd
import numpy as np

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import SelectFromModel
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import GridSearchCV

df  = pd.read_csv("E:/E2E_DTP_Project/data/final_feature_engg_master.csv")

null_rows = df[df.isnull().any(axis=1)]
print("Rows with null, NaN, or None values:")
print(null_rows)

null_columns = df.columns[df.isnull().any()]
print("\nColumns with null, NaN, or None values:")
print(null_columns)

df['quantity'] =df['quantity'].fillna(df['quantity'].mean())

X = df.drop(['estimated_stock_percent','product_id','timestamp'], axis=1)  # Replace 'target_variable' with the name of your target variable column
y = df['estimated_stock_percent']

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
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)



feature_selector = SelectFromModel(RandomForestRegressor(n_estimators=100))
X_train_selected = feature_selector.fit_transform(X_train, y_train)
X_test_selected = feature_selector.transform(X_test)

# Hyperparameter tuning using GridSearchCV
param_grid = {
    'n_estimators': [100, 200, 300],
    'max_depth': [None, 5, 10],
    'min_samples_split': [2, 5, 10]
}
grid_search = GridSearchCV(RandomForestRegressor(), param_grid, cv=5)
grid_search.fit(X_train_selected, y_train)
best_model = grid_search.best_estimator_

# Model evaluation
y_pred = best_model.predict(X_test_selected)
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)
rmse = np.sqrt(mse)

print(f"Mean Squared Error: {mse:.2f}")
print(f"RMSE: {rmse:.2f}")
print(f"R-squared: {r2:.2f}")