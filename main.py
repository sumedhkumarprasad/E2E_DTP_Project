import argparse
import logging
import yaml
from src.utils import upload_to_s3, process_task, upload_to_googlesheet

args = argparse.ArgumentParser(
    description="Provies some inforamtion on the job to process"
)
args.add_argument(
    "-t", "--task", type=str, required=True,
    help="This will point to a task location into the config.yaml file.\
        Then it will follow the step of this specific task.")
args = args.parse_args()

with open("./config/config.yaml", 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

#args_task = 'Model_building'
config_export = config[args.task]["export"]

if config_export[0]["export"]["host"] == 's3':
    if args.task == 'final_cleaned_python_sql':
        processed_data = process_task(args.task)
        if processed_data is not None:
            export_config = config_export[0]["export"]
            upload_to_s3(processed_data, export_config["filename"], export_config["bucket"])
            print(f'Uploaded final_cleaned_python_sql to {export_config["bucket"]}')
        else:
            logging.error('Processed data for final_cleaned_python_sql is None.')
    elif args.task == 'feature_engg_scripts':
        ml_output_data = process_task(args.task)
        if ml_output_data is not None:
            export_config = config_export[0]["export"]
            upload_to_s3(ml_output_data, export_config["filename2"], export_config["bucket"])
            print(f'Uploaded feature engg data  to {export_config["bucket"]}')
        else:
            logging.error('Processed data for Model_building is None.')
elif config_export[0]["export"]["host"] == 'gsheet':
    if args.task == 'Model_building':
        historical_data, final_prediction_data, feature_importance_data, metrics_df = process_task(args.task)
        if historical_data is not None:
            export_config = config_export[0]["export"]
            upload_to_googlesheet(historical_data, export_config["spread_sheet_id"], export_config["worksheet_name"], export_config["filename1"])
            print('Uploaded historical data')
        else:
            logging.error('Historical data is None.')

        if final_prediction_data is not None:
            export_config = config_export[0]["export"]
            upload_to_googlesheet(final_prediction_data, export_config["spread_sheet_id"], export_config["worksheet_name"], export_config["filename2"])
            print('Uploaded final prediction data')
        else:
            logging.error('Final prediction data is None.')

        if feature_importance_data is not None:
            export_config = config_export[0]["export"]
            upload_to_googlesheet(feature_importance_data, export_config["spread_sheet_id"], export_config["worksheet_name"], export_config["filename3"])
            print('Uploaded feature importance data')
        else:
            logging.error('Feature importance data is None.')
        
        if metrics_df is not None:
            export_config = config_export[0]["export"]
            upload_to_googlesheet(metrics_df, export_config["spread_sheet_id"], export_config["worksheet_name"], export_config["filename4"])
            print('Uploaded metrics data')
        else:
            logging.error('Feature importance data is None.')
    else:
        logging.error('Invalid task specified.')
else:
    logging.error('Invalid export host specified.')
