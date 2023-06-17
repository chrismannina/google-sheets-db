import gspread
import pandas as pd


def get_google_sheet(file, sheet):
    """
    Connect to Google Sheets file and return the data from the specified sheet as a pandas DataFrame.
    
    Parameters:
    - file: The Google Sheet file name.
    - sheet: The worksheet name in the file.
    
    Returns:
    - df: A pandas DataFrame with the worksheet data.
    - worksheet: The gspread worksheet object.
    """
    gc = gspread.oauth()
    sh = gc.open(file)
    worksheet = sh.worksheet(sheet)
    records = worksheet.get_all_records()  # returns list of dictionaries
    df = pd.DataFrame(records)
    return df, worksheet


def update_google_sheet(df_google, df_pharmdw, worksheet, merge_keys, google_cols, pharmdw_cols, opt_updates=None, debug=False):
    """
    Update Google Sheets with any new data found from CR.

    Parameters:
    - df_google: The current Google Sheets data as a pandas DataFrame.
    - df_pharmdw: The current pharmDW table data as a pandas DataFrame.
    - worksheet: The gspread worksheet object to update.
    - key: The column name that is common in both dataframes for comparison.
    - pharmdw_cols: List of columns in pharmDW data to be considered for new data.
    - google_cols: List of columns in Google Sheets data to be updated.
    - opt_updates:  A dictionary where keys are type of additional update and values are specific instructions. 
        Keys can are the column name, except 'function'. This adds additional functionality to insert a row number.
    - debug: If True, print debug information.
    
    Example usage:
    update_google_sheet(df_google, df_cr, worksheet, 'schedule_id',
                    ['shift_group_name', 'location_name', 'schedule_id', 'schedule_name'], 
                    ['Humanity_Account', 'Humanity_Location', 'schedule_id', 'Position_Name'], 
                    {"function": {"division_id": "=ARRAYFORMULA(INDEX(Division_Lookup!A:A, MATCH(H{row}, Division_Lookup!B:B, 0)))",
                                "area_id": "=ARRAYFORMULA(INDEX(Area_Lookup!A:A, MATCH(E{row}&I{row}, Area_Lookup!C:C&Area_Lookup!B:B, 0)))"},
                    "Notes":"New position. Please fill out columns G, H, I.",
                    "updated_by":"automated script"},
                    debug=True)

    """
    # Identify new IDs that are in the CR data but not in the Google Sheets data
    merged_df = pd.merge(df_google, df_pharmdw, how='outer', indicator=True, left_on=merge_keys, right_on=merge_keys)
    new_data = merged_df[merged_df['_merge'] == 'right_only']
    new_ids = new_data[pharmdw_cols]
    new_ids.columns = google_cols

    if new_ids.empty:
        print('No new data to add.') if debug else None
        return False
    
    for _, row in new_ids.iterrows():
        # Append the new row to the worksheet
        row_to_append = [row[col] for col in google_cols]
        worksheet.append_row(row_to_append)

        # Check if there are any optional updates
        if opt_updates:
            # Get header and last row
            header_row = worksheet.row_values(1)
            last_row = len(worksheet.get_all_values())
            for update_key, update_val in opt_updates.items():
                if update_key == 'function':       
                    for col_name, formula_template in update_val.items():
                        col_index = header_row.index(col_name) + 1
                        formula = formula_template.format(row=last_row)
                        worksheet.update_cell(last_row, col_index, formula)
                else:
                    # Fetching the header row from the worksheet
                    header_row = worksheet.row_values(1)
                    # Finding the index of update_type column (e.g. Notes)
                    col_index = header_row.index(update_key) + 1
                    worksheet.update_cell(last_row, col_index, update_val)
    print('Google Sheet updated.') if debug else None
    return True

