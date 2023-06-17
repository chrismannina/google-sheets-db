from src import db_access, gsheet_access, file_io, comparison_utils, read_config
import numpy as np
import json


def main(configs):
    for config in configs:
        print('Beginning {task}...'.format(task=config['task'])) if config['debug'] else None
        
        # Get google worksheet and saved in a dataframe
        df_google, worksheet = gsheet_access.get_google_sheet(config['df_google']['file'], config['df_google']['sheet']) 
        df_google = df_google.replace([np.inf, -np.inf], np.nan)
        print(df_google) if config['debug'] else None    
        
        # Get pharmdw table and save in data frame
        df_pharmdw = db_access.dw_call(config['df_pharmdw']['select_query'])
        df_pharmdw = df_pharmdw.replace([np.inf, -np.inf], np.nan)
        print(df_pharmdw) if config['debug'] else None
        
        # Backup pharmdw table if option enabled (if 'enabled' is not found, it returns False)
        if config.get('backup_cr', {}).get('enabled', False):
            file_io.save_backup(df_pharmdw, config['save_backup']['filename'], config['save_backup']['filepath'], config['debug'])
            
        # Update table in pharmdw if option enabled with any changes that were made from google sheet
        if config.get('update_pharmdw', {}).get('enabled', False):
            update_keys = comparison_utils.find_updated_keys(df_google, df_pharmdw, config['update_pharmdw']['primary_key'], config['update_pharmdw']['column_mapping'], debug=config['debug'])
            if update_keys:
                print('Updating... ', update_keys) if config['debug'] else None
                comparison_utils.update_table(update_keys, df_google, config['update_pharmdw']['primary_key'], config['update_pharmdw']['update_query'], config['update_pharmdw']['update_params'], debug=config['debug'])
            
        # Update google sheet if option enabled with any new rows from CR that were identified 
        if config.get('update_google', False):
            gsheet_access.update_google_sheet(df_google, df_pharmdw, worksheet, config['update_google']['merge_on'], config['update_google']['google_cols'], config['update_google']['pharmdw_cols'], config['update_google']['opt_updates'], config['debug'])
        

        print('Completed {task}.'.format(task=config['task'])) if config['debug'] else None

    
if __name__ == '__main__':
    config_path = 'config.json'     # set config.json path and file name here
    with open(config_path, 'r', encoding='utf-8-sig') as f:
        main(json.load(f))