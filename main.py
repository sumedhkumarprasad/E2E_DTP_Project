import argparse
import logging
import yaml
from src.utils import upload_to_s3, process_task #, upload_to_google_sheet

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

#args_task = 'final_cleaned_python_sql'
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
    elif args.task == 'Model_building':
        ml_output_data = process_task(args.task)
        if ml_output_data is not None:
            export_config = config_export[0]["export"]
            upload_to_s3(ml_output_data, export_config["filename2"], export_config["bucket"])
            print(f'Uploaded Model_building to {export_config["bucket"]}')
        else:
            logging.error('Processed data for Model_building is None.')
elif config_export[0]["export"]["host"] == 'gsheet':
    pass
    #upload_to_google_sheet(config_export[0]["export"]["spread_sheet_id"], 
     #                      process_task(args.task), 
      #                     config_export[0]["export"]["worksheet_name"])
