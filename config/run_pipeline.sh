#!/bin/bash

# load data from csv files into a database
echo "Creating raw database......."
python src/database.py -host 'localhost' -user 'root' -db True -dbname 'raw_data'

# normalize and clean data, and upload to database
echo "Creating table & uploading data......."
python src/database.py -host 'localhost' -user 'root' -dbname 'raw_data' -id 'upload-to-database'

echo "Running ETL......."
python src/ETL.py

echo "Creating processed database......."
python src/database.py -host 'localhost' -user 'root' -db True -dbname 'processed_data'

echo "Creating table & uploading data......."
python src/database.py -host 'localhost' -user 'root' -dbname 'processed_data' -id 'cleaned-upload-to-database'

# train and evaluate machine learning models
echo "Extracting data & uploading to S3......."
python3 main.py -t final_cleaned_python_sql
python3 main.py -t feature_engg_scripts

echo "Running modelling & uploading to gsheet......."
python3 main.py -t Model_building