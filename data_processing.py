import numpy as np
import pandas as pd

from typing import List, Dict
from utils import replacing_string_char


def func(value):
    if value is ' ':  # Turning a ' ' (space) char into 'nan' (so afterwards it could be aggregated)
        return pd.to_numeric(value, errors='coerce')
    if (pd.to_numeric(value, errors='coerce') > -np.inf) and int(value) >= 1e3:
        return int(value) / 1e1
    return value


def wrong_data_filtering(data_frame):
    # Wrong Data; Correcting all misleading data by a Decade (log scale - log_10(P) = x => P=1ex)
    # Turning all non-numeric values into 'nan'
    data_frame = data_frame.applymap(lambda value: func(value))

    return data_frame


def removing_duplicates(data_frame):
    # Removing Duplicates
    data_frame.drop_duplicates(inplace=True)

    return data_frame


def interpolated_data(data_frame):  # Replacing 'nan' values with the desired interpolation
    clean_data_frame = data_frame.copy()
    # Changes all the values of the non-numeric cells to 'nan. The result is pandas DataFrame
    clean_data_frame = clean_data_frame.apply(lambda series: pd.to_numeric(series, errors='coerce'))
    # Applying an aggregation function on the DataFrame. The result is pandas series - size: (number_of_columns,)
    interpolation_data_series = clean_data_frame.aggregate(func=np.mean, axis=0)  # TODO: Try multiple functions
    # Filling the 'nan' cells with the resulted pandas series above (Because the series above and the DataFrame are the
    # same size, the appliance will be done by matching each value in the 'interpolation_data_series' on it's
    # corresponding series (column) in the DataFrame
    data_frame.fillna(values=interpolation_data_series, inplace=True)

    return data_frame


def data_filtering(data_frame_list: List[pd.DataFrame]):
    for list_index, data_frame in enumerate(data_frame_list):
        data_frame = wrong_data_filtering(data_frame)
        data_frame = removing_duplicates(data_frame)
        data_frame = interpolated_data(data_frame)
        data_frame_list[list_index] = data_frame

    return data_frame_list


def non_integer_type_columns(concatenated_table: pd.DataFrame) -> (List, List):
    # Picking the non-integer type columns
    numeric_columns_list, non_numeric_columns_list = [], []
    for col in concatenated_table.columns:
        # Dropping Null cells
        non_nan_df = concatenated_table[col]
        non_nan_df.dropna(inplace=True)
        non_nan_df.reset_index(drop=True, inplace='coerce')

        numeric_df = pd.to_numeric(non_nan_df, errors='coerce')
        if np.sum(numeric_df > -np.inf) != len(non_nan_df):
            non_numeric_columns_list.append(col)
            # continue
        # elif concatenated_table[col][0] > -np.inf:
        #     numeric_column_list.append(col)

    return numeric_columns_list, non_numeric_columns_list


def create_clean_data_frame(data_frame: pd.DataFrame) -> pd.DataFrame:
    # Create clean DataFrame
    drop_clean_set: set = {'-', '_', '/', '\\'}
    clean_data_frame = data_frame.copy()
    for column in clean_data_frame.keys():
        for index, cell in enumerate(clean_data_frame[column]):
            cell = str(cell)
            cell = list(cell)
            cell = [ch for ch in cell if ch not in drop_clean_set]

            # Joining back into string
            s: str = ''
            for ch in cell:
                s += ch
            cell = s
            clean_data_frame.loc[index, column] = cell

    clean_data_frame.replace('nan', np.nan, inplace=True)
    return clean_data_frame


def pandas_to_numeric(data_frame: pd.DataFrame) -> pd.DataFrame:
    for col in data_frame.columns:
        for index, cell in enumerate(data_frame[col]):
            numeric_value = pd.to_numeric(cell, errors='coerce')
            if numeric_value > -np.inf:  # Is numeric
                data_frame.loc[index, col] = float(numeric_value)
        # data_frame[col] = pd.to_numeric(data_frame[col], errors='ignore')

    return data_frame


def dropping_nan_columns(data_frame: pd.DataFrame):
    # Dropping NAN Columns
    data_frame.dropna(axis=1, how='all', inplace=True)
    data_frame.reset_index(drop=True, inplace=True)


def dropping_nan_rows(data_frame: pd.DataFrame):
    # Dropping NAN Rows
    data_frame.dropna(axis=0, how='all', inplace=True)
    data_frame.reset_index(drop=True, inplace=True)


"""
def dropping_nan_columns(data_frame: pd.DataFrame):
    # Dropping NaN colmns
    for col in data_frame.keys():
        if data_frame[col].isna().values.sum() == len(data_frame[col]):  # If a certain column is NaN
            data_frame.drop(columns=[col], inplace=True) 


def dropping_nan_rows(data_frame: pd.DataFrame):
    # Dropping NaN Rows
    index_list_to_drop: List = []
    for index, row in data_frame.iterrows():
        if row.isna().values.sum() == len(row):  # If a certain row is NaN
            inedx_list_to_drop.append(index)
    data_frame.drop(index_list_to_drop, inplace=True)
    data_frame.reset_index(drop=True, inplace=True)
"""


def changing_column_indexes(data_frame):
    # Changing columns Indexes to relevant ones
    is_nan = False
    while True:
        unnamed_columns_count = 0
        for column in data_frame.columns:
            if type(column) is tuple:
                is_nan = column[0] is np.nan
            if ('Unnamed' in column) or is_nan:
                unnamed_columns_count += 1
        # Rule for suspecting that first row is not the main file index
        if unnamed_columns_count >= len(data_frame.columns) - 2:
            data_frame.columns = [data_frame.loc[0]]
            data_frame.drop(0, inplace=True)
            data_frame.reset_index(drop=True, inplace=True)
        else:
            break


def changing_column_indexes_names(data_frame):
    # Replacing '\n' and ' ' with '_' in DataFrame's indexes names
    columns_list: List = []
    for column in data_frame.columns:
        column_name = column[0]
        column_name = column_name.replace('\n', '_')
        i = 0
        while i < len(column_name):
            if column_name[i] == ' ':
                if (i != 0) and (i != len(column_name) - 1) and column_name[i+1] != '_' and column_name[i-1] != '_':
                    # Replacing character column_name[i] with '_'
                    column_name = replacing_string_char(column_name, i, '_')
                    continue
                # Deleting column[i]
                column_name = replacing_string_char(column_name, i, None)
            i += 1

        # If the first or last char was '\n', then delete the '_' char which it was replaced with
        while column_name[0] == '_' or column_name[-1] == '_':
            if column_name[0] == '_':
                column_name = replacing_string_char(column_name, 0, None)
                continue
            column_name = replacing_string_char(column_name, -1, None)

        columns_list.append(column_name)
    data_frame.columns = columns_list


def data_frame_cleaning(data_frame):
    dropping_nan_columns(data_frame)

    dropping_nan_rows(data_frame)

    changing_column_indexes(data_frame)

    changing_column_indexes_names(data_frame)


def create_data_frames(file: str, pandas_callback_function: callable):
    data_frame_list: List = []
    file_name_list: List = []

    # FIXME: wrap the pandas_callback_function() with decorators in the extensions dictionary
    if pandas_callback_function is pd.read_csv:  # If it's a csv file

        # TODO: Create a function for this - return func(file, pandas_callback_function)
        data_frame: pd.DataFrame = pandas_callback_function(file)

        data_frame_cleaning(data_frame)

        data_frame_list.append(data_frame)

        # Determining SQL table's name
        file_name_with_extension = file.split('\\')[-1]
        file_name: str = file_name_with_extension.split('.')[0]
        file_name_list.append(file_name)

    else:  # If it's an excel file
        # TODO: Create a function for this - return func(file, pandas_callback_function)
        data_dict: Dict = pandas_callback_function(file, sheet_name=None)
        for key in data_dict.keys():  # For each sheet in the Excel file
            data_frame: pd.DataFrame = data_dict[key]  # Create a data frame from each Excel sheet

            data_frame_cleaning(data_frame)

            data_frame_list.append(data_frame)
            file_name: str = key  # .replace(' ', '_')  # Determining SQL table's name - sheet's name
            file_name_list.append(file_name)

    return file_name_list, data_frame_list
