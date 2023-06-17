from datetime import datetime
import pandas as pd
import os


def save_backup_cr(df_cr, file_name, file_path='.', debug=False):
    """
    Save a backup of the CR data to a CSV file.

    Parameters:
    - df_cr: The CR data as a pandas DataFrame.
    - file_name: Desired file name for output CSV. Current date will be prefixed (YYYYMMDD_file_name.csv).
    - path: The directory where the CSV file will be saved. Defaults to the current directory.
    - debug: If True, print debug information.
    """
    filename = f'{datetime.now().strftime("%Y%m%d")}_{file_name}.csv'
    file_path = os.path.join(file_path, filename)

    try:
        df_cr.to_csv(file_path, index=False)
        if debug:
            print(f'Successfully saved CR to {file_path}')
    except Exception as e:
        print(f"An error occurred while trying to save the file: {e}")
