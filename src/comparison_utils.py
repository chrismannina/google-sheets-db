import pandas as pd
from .db_access import dw_execute
import re


def find_updated_keys(df_google, df_pharmdw, primary_key, column_mapping, debug=False):
    """
    Check if there are any updates in the Google Sheets data compared to the database table.

    Parameters:
    - df_google: The current Google Sheets data as a pandas DataFrame.
    - df_pharmdw: The current database table data as a pandas DataFrame.
    - merge_on: The keys on which to merge the dataframes.
    - google_to_pharmdw_col_map: A dict mapping each column in df_google to its corresponding column in df_pharmdw.
    - debug: If True, print debug information.

    Returns:
    - A list of IDs that need to be updated.
    """
    # Fill NaN values with an empty string
    df_google = df_google.fillna('')
    df_pharmdw = df_pharmdw.fillna('')
    
    # merge the two dataframes on 'id_column' with inner join - this is to only check the positions we already had
    merged = pd.merge(df_google, df_pharmdw, on=primary_key, how='inner')
    print(merged) if debug else None

    # Initialize a list to store ids with differences
    diffs_ids = []

    # Iterate over each row in the merged dataframe
    for _, row in merged.iterrows():
        # Iterate over the column_mapping dictionary
        for col_df1, col_df2 in column_mapping.items():
            # If the column names are the same in df1 and df2, add suffix to col_df2
            if col_df1 in df_google.columns and col_df1 in df_pharmdw.columns:
                col_df1 += '_x'
                col_df2 += '_y'
            # If the values in the corresponding columns are different
            if row[col_df1] != row[col_df2]:
                print(col_df1, row[col_df1], col_df2, row[col_df2])
                # Append the id of this row to 'diffs_ids'
                diffs_ids.append(row[primary_key])
                # No need to check other columns if a difference has been found, so break the inner loop
                break
    print(diffs_ids) if debug else None

    # Return the 'id_column's of these rows without duplicates
    return list(set(diffs_ids))


def update_table(ids_to_update, df, id_column, update_query, update_params, debug=False):
    """Update rows in a table based on a list of ids and a dataframe."""

    # Filter df for the relevant ids
    updates = df[df[id_column].isin(ids_to_update)][[id_column] + update_params]

    # Iterate over each row (i.e., each id to be updated)
    for _, row in updates.iterrows():
        # Check if any value in the row is not alphanumeric
        if not all(re.match("^[a-zA-Z0-9]*$", str(value)) for value in row.values):
            print(f"Skipping update for row:\n{row}\n") if debug else None
            continue
        # Prepare the parameters for the UPDATE statement
        params = {param: row[param] for param in update_params}
        params[id_column] = row[id_column]

        # Execute the UPDATE statement
        # dw_update(update_query, params)
        print(update_query, params) if debug else None

    print(f'Successfully updated the table.') if debug else None


# def update_db(ids_to_update, df, primary_key, sql_query, sql_params, debug=False):
#     """
#     Update the DB data for the given IDs.

#     Parameters:
#     - ids_to_update: A list of IDs to update.
#     - df: The current data as a pandas DataFrame.
#     - sql_query: The SQL query to update the database.
#     - sql_params: The parameters for the SQL query.
#     - merge_keys: The keys to filter rows from the dataframe.
#     - debug: If True, print debug information.
    
#     Usage:
#     update_db([['schedule_id_1', 'schedule_id_2'], ['employee_type_1', 'employee_type_2']], df, sql_query, sql_params, 
#     ['schedule_id', 'employee_type'])
#     """
#     # Start by filtering based on the first key
#     mask = df[merge_keys[0]].isin(ids_to_update[0])
#     print(mask)


#     # Further filter based on the other keys, if any
#     for key, ids in zip(merge_keys[1:], ids_to_update[1:]):
#         mask = mask & df[key].isin(ids)

#     updates = df[mask][merge_keys + sql_params]

#     print(updates)

#     # Update each ID in the database
#     for _, row in updates.iterrows():
#         print(row)
#         query_params = {param: row[param] for param in sql_params}
#         print(query_params)
#         # dw_execute(sql_query, query_params)

#     if debug:
#         print(f'Successfully updated the table for {len(updates)} rows.')


# def update_pharmdw(update_keys, df_google, merge_on, google_cols, update_query, update_params=None, debug=False):
#     """
#     Update the CR data for the given IDs.

#     Parameters:
#     - ids_to_update: A list of IDs to update.
#     - df_google: The current Google Sheets data as a pandas DataFrame.
#     - sql_query: The SQL query to update the database.
#     - sql_params: The parameters for the SQL query.
#     - merge_keys: The keys to filter rows from the dataframe.
#     - set_method: The method to set the data to the database. It can be "dw_update" or "dw_insert"
#     - debug: If True, print debug information.
    
#     Usage:
#     update_cr([['schedule_id_1', 'schedule_id_2'], ['employee_type_1', 'employee_type_2']], df_google, sql_query, sql_params, 
#     ['schedule_id', 'employee_type'])
#     """
#     # Start by filtering based on the first key
#     mask = df_google[merge_on[0]].isin(update_keys[0])

#     # Further filter based on the other keys, if any
#     for key, ids in zip(merge_on[1:], update_keys[1:]):
#         mask = mask & df_google[key].isin(ids)

#     # To avoid raising a 'KeyError' when accessing a key that doesn't exist, we set a default value of []
#     updates = df_google[mask][merge_on + update_params]
    
#     print(updates) if debug else None

#     # Depending on the method, perform the appropriate action
#     if set_method == "update":
#         # Update each ID in the database
#         for _, row in updates.iterrows():
#             params = {param: row[param] for param in update_query}
#             print('query: ', sql['update_query']) if debug else None
#             print('params: ', params) if debug else None
#             # dw_execute(sql['update_query'], params)
#     elif set_method == "insert":
#         # TODO: add code for trunc and insert here
#         pass 
#     else:
#         print('Error: check set_method option.')

#     print(f'Successfully updated the CR table for {len(updates)} rows.') if debug else None
