from typing import List, Dict, Optional
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, NVARCHAR, exc, NUMERIC
from sqlalchemy.dialects.mssql import DATETIME2
import config
import sys
import json
import hashlib
import pathlib
from _datetime import datetime


def get_extensions():
    csv_extensions: Dict = dict.fromkeys(['.csv'], pd.read_csv)
    excel_extensions: Dict = dict.fromkeys(['.xlxs', 'xlsm', '.xlsb', '.xltx', 'xltm', 'xls', '.xlt', '.xml',
                                            '.xlam', '.xla', '.xlw', '.xlr'], pd.read_excel)
    extensions: Dict = {**csv_extensions, **excel_extensions}

    return extensions


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


def create_view_sql_query(view_table_name, translate_dict, table_name, sql_columns_names):
    raw_view_query = f'Create View [raw].[V_{view_table_name}] as '
    clean_view_query = f'Create View [clean].[V_{view_table_name}] as '
    raw_view_query += f'(Select [index], '
    clean_view_query += f'(Select [index], '

    for column in sql_columns_names:
        if column[0] in translate_dict.keys():
            word_index = column[0]
            translated_word = translate_dict[column[0]]
            raw_view_query += f'[{word_index}] as [{translated_word}], '
            clean_view_query += f'[{word_index}] as [{translated_word}], '
    for column in sql_columns_names:
        if column[0] not in translate_dict.keys() and column[0] != 'index':
            raw_view_query += f'[{column[0]}], '
            clean_view_query += f'[{column[0]}], '

    raw_view_query = raw_view_query.rstrip(', ')  # Strip the last comma out of the raw_view_query
    clean_view_query = clean_view_query.rstrip(', ')  # Strip the last comma out of the raw_view_query
    raw_view_query += ' '
    clean_view_query += ' '
    raw_view_query += f'From [{config.db_name}].[raw].[{table_name}]);'
    clean_view_query += f'From [{config.db_name}].[clean].[{table_name}]);'

    return raw_view_query, clean_view_query


def is_relevant_table(translate_dict, sql_columns_names):
    relevant_table: bool = False
    for column in sql_columns_names:
        if translate_dict.get(column[0], None):
            relevant_table = True
            break
    return relevant_table


def create_engine_path() -> str:
    engine_path: str = f'{config.sql_server}+{config.accessing_library}://{config.mssql_username}:{config.password}' \
                       f'@{config.server_name}:{config.server_conn_port}/{config.db_name}?driver={config.driver_name}'
    return engine_path


def create_sql_view_tables(translate_dict: Dict):
    engine_path: str = create_engine_path()
    engine = create_engine(engine_path, echo=False)

    with engine.begin() as conn:
        sql_table_names = conn.execute(f'Select table_name From {config.db_name}.INFORMATION_SCHEMA.TABLES').fetchall()
        for table in sql_table_names:
            table_name = table[0]
            if table_name[:2] == 'V_' or table_name == 'Files_Meta_Data':
                continue

            sql_columns_names = conn.execute(f'Select distinct column_name '
                                             f'From {config.db_name}.INFORMATION_SCHEMA.COLUMNS '
                                             f'WHERE table_name = N\'{table_name}\'').fetchall()
            relevant_table: bool = is_relevant_table(translate_dict, sql_columns_names)
            if relevant_table:
                view_table_name = table_name.replace(' ', '_')

                raw_view_query, clean_view_query = create_view_sql_query(view_table_name, translate_dict, table_name,
                                                                         sql_columns_names)

                # Dropping the View table if it has been already created (to be able to insert a new one)
                conn.execute(f'Drop View if exists [raw].[V_{view_table_name}]')
                conn.execute(f'Drop View if exists [clean].[V_{view_table_name}]')
                # Creating a new View table
                conn.execute(raw_view_query)
                conn.execute(clean_view_query)


def merge_dictionaries(dict1, dict2) -> Dict:
    merged_dictionary: Dict = {**dict1, **dict2}

    return merged_dictionary


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


def add_to_db(data_frame_list: List[pd.DataFrame], table_name_list: List[str]):
    for table_name, data_frame in zip(table_name_list, data_frame_list):

        engine_path: str = create_engine_path()
        engine = create_engine(engine_path, echo=False)

        try:  # Check if the table {table_name} already exists
            existing_table: Optional[pd.DataFrame] = pd.read_sql(f'SELECT * FROM [raw].[{table_name}];', engine)
            existing_table.drop(columns=[existing_table.columns[0]], inplace=True)  # Dropping the SQL's 'index' column
        except BaseException as exc:  # If this is the first insertion
            print(f'\nAn {exc} Has Occurred!\n')
            print(f'There wasn\'t a SQL Table by name: {table_name}!\n')
            print(f'Creating new one...\n')
            existing_table = None

        # Take table from mssql, concat with data_frame (ignore_index=True), drop duplicates, send again to sql
        """
        IMPORTANT:
        We're reading the SQL table to the local machine RAM (as Panda's DataFrame), concatenating it (if exists)
        with the current data_frame, and then sending it back to mssql.
        
        Another option was to insert the current data_frame as Upserting (Insert-Update) Panda's to SQL command.
        While it's more practical, faster, reducing usage of memory (RAM), and back_forth process saving. In case
        there's a current data_frame with different column names then the existing one (Table) in mssql, it wouldn't
        work! (the insertion command) - SQL doesn't know how to update table's columns.
        
        For this reason, it was decided to use Panda's 'Concat' command (which required reading the SQL Table,
        concatenating it locally, and then updating (dropping) the existed one on mssql).
        """
        concatenated_table = pd.concat([existing_table, data_frame], ignore_index=True)  # Reordering the rows indexes
        # concatenated_table = pd.to_numeric(concatenated_table, errors='coerce')
        # concatenated_table = concatenated_table.apply(lambda pd.to_numeric(series, errors='ignore') for series in)
        # concatenated_table = pandas_to_numeric(concatenated_table)
        concatenated_table.replace(0, '0', inplace=True)
        # concatenated_table.fillna(np.nan, inplace=True)
        concatenated_table.fillna('np.nan', inplace=True)
        # concatenated_table = concatenated_table.applymap(str)
        concatenated_table = pandas_to_numeric(concatenated_table)
        concatenated_table.drop_duplicates(inplace=True, ignore_index=True)
        concatenated_table.replace('np.nan', np.nan, inplace=True)
        # concatenated_table = pandas_to_numeric(concatenated_table)
        # concatenated_table = pd.to_numeric(concatenated_table, errors='coerce')

        # Create clean DataFrame
        clean_data_frame = create_clean_data_frame(concatenated_table)

        # _, non_numeric_columns_list = non_integer_type_columns(concatenated_table)

        # Changing non-integer columns type (in sql) to NVarChar, and integer ones to NUMERIC
        # (for future interpolation on data)
        # nvarchar_dict: Dict = {col_name: NVARCHAR for col_name in non_numeric_columns_list}
        nvarchar_dict: Dict = {col_name: NVARCHAR for col_name in concatenated_table.columns}
        # numeric_dict: Dict = {col_name: NUMERIC for col_name in numeric_columns_list}
        # nvarchar_dict.update(numeric_dict)
        # merged_dict = merged_dictionaries(nvarchar_dict, numeric_dict)

        concatenated_table.to_sql(table_name, engine, if_exists='replace', dtype=nvarchar_dict, schema='Raw')

        clean_data_frame.to_sql(table_name, engine, if_exists='replace', dtype=nvarchar_dict, schema='Clean')


def data_filtering(data_frame_list: List[pd.DataFrame]):
    for list_index, data_frame in enumerate(data_frame_list):
        data_frame = wrong_data_filtering(data_frame)
        data_frame = removing_duplicates(data_frame)
        data_frame = interpolated_data(data_frame)
        data_frame_list[list_index] = data_frame

    return data_frame_list


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


def replacing_string_char(name: str, index: int, replace_char: Optional[str] = None):
    name = list(name)
    name[index] = replace_char
    if not replace_char:
        name.pop(index)

    # Joining back into string
    s: str = ''
    for ch in name:
        s += ch
    name = s
    return name


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


def create_data_frame(file: str, pandas_callback_function: callable):
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


def calculate_md5_hash(file_path: str) -> str:
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as binary_file:
        binary_file_content = binary_file.read()
        md5_hash.update(binary_file_content)
        file_md5_hash = md5_hash.hexdigest()

    return file_md5_hash


def read_json_translation_file(translate_index_file_path: str):
    with open(translate_index_file_path, 'r', encoding='utf-8') as json_file:
        translation_dictionary: Dict[str, str] = json.load(json_file)
    return translation_dictionary


def file_translation_dictionary_path(file_mapping_directory, file, file_name):
    translate_index_file_path = None  # Initial assignment
    for _, _, mapping_file_List in os.walk(file_mapping_directory):
        translate_index_file = f'{file_name}.json'
        translate_index_file_path = os.path.join(file_mapping_directory, translate_index_file)
        if translate_index_file not in mapping_file_List:
            print(f'\nWarning...\n')
            print(f'There is no translating dictionary for {file}')
            print(f'\n\nApplying the default translation dictionary!\n')
            translate_index_file_path = f'{config.path_mapping}\\Oxford_Dictionary_Translation.json'
        break

    return translate_index_file_path


def translation_dictionary_path(file_mapping_directory):
    translate_index_file_path = None  # Initial assignment
    for _, _, mapping_file_List in os.walk(file_mapping_directory):
        translate_index_file = 'Oxford_Dictionary_Translation.json'
        translate_index_file_path = os.path.join(file_mapping_directory, translate_index_file)
        if translate_index_file not in mapping_file_List:
            print(f'\nWarning...\n')
            print(f'There is no translating dictionary in the provided directory')
            print(f'\n\nApplying the default translation dictionary!\n')
            translate_index_file_path = f'{config.path_mapping}\\Oxford_Dictionary_Translation.json'
        break

    return translate_index_file_path


def fetching_sql_file_meta_data_table(engine):
    # Fetching SQL File_Meta_Data Table
    try:  # Check if the table 'Meta_Data' already exists
        existing_table: Optional[pd.DataFrame] = pd.read_sql(f'SELECT * FROM [dbo].[Files_Meta_Data];', engine)
        existing_table.drop(columns=[existing_table.columns[0]],
                            inplace=True)  # Dropping the SQL's 'index' column
    except exc.ProgrammingError as e:  # If this is the first insertion
        print(f'\nAn {e} has Occurred!\n')
        print(f'There wasn\'t a SQL Table by name: Meta_Data!\n')
        print(f'Creating one...\n')
        existing_table = None

    return existing_table


def file_crawler(root_directory: str, file_mapping_directory: str, apply_data_filters: bool):
    list_csv_files: List[str] = []
    extension_types = get_extensions()
    file_names_list, file_path_list, modification_date_list, creation_date_list, file_md5_list = [], [], [], [], []

    # Fetching SQL File_Meta_Data Table
    engine_path: str = create_engine_path()
    engine = create_engine(engine_path, echo=False)
    existing_table = fetching_sql_file_meta_data_table(engine)

    # Fetching the all file's columns translation dictionary
    translate_index_file_path = translation_dictionary_path(file_mapping_directory)

    for dir_name, sub_dir_list, file_List in os.walk(root_directory):
        for file in file_List:
            file_name, extension = os.path.splitext(file)
            extension = extension.lower()
            if extension in extension_types.keys():
                # Fetching the file's columns translation dictionary
                # Translate_index_file_path = file_translation_dictionary_path(file_mapping_directory, file, file_name)

                translate_dict: Dict = read_json_translation_file(translate_index_file_path)
                file_path = os.path.join(dir_name, file)

                file_information = pathlib.Path(file_path)  # All file's information
                modification_time = file_information.stat().st_mtime  # Modification time (in [sec])
                modification_date = datetime.fromtimestamp(modification_time)  # Modification Date (in date view)
                creation_time = file_information.stat().st_ctime  # Creation time (in [sec])
                creation_date = datetime.fromtimestamp(creation_time)  # Creation Date (in date view)

                if file_name[0:2] == '~$':  # If the file is currently open
                    continue
                try:
                    if ((existing_table['File_name'] == file_name) &
                        (existing_table['File_path'] == file_path) &
                        (existing_table['Modification_date'] == modification_date) &
                        (existing_table['Creation_date'] == creation_date)).any():
                        continue
                except TypeError as e:
                    pass

                file_md5_hash = calculate_md5_hash(file_path)
                try:
                    if (existing_table['File_md5'] == file_md5_hash).any():
                        continue
                except TypeError as e:
                    pass

                file_names_list.append(file_name)
                file_path_list.append(file_path)
                modification_date_list.append(modification_date)
                creation_date_list.append(creation_date)
                file_md5_list.append(file_md5_hash)

                file_name_list, data_frame_list = create_data_frame(file_path, extension_types[extension])

                # data_frame_list = data_filtering(data_frame_list)  # TODO: Change to be choosable
                add_to_db(data_frame_list, file_name_list)
                create_sql_view_tables(translate_dict)
                list_csv_files.append(file_path)

    file_information_data_frame = pd.DataFrame({'File_name': file_names_list,
                                                'File_path': file_path_list,
                                                'Modification_date': modification_date_list,
                                                'Creation_date': creation_date_list,
                                                'File_md5': file_md5_list})
    updated_data_frame = pd.concat([existing_table, file_information_data_frame], ignore_index=True)
    updated_data_frame.drop_duplicates(inplace=True, ignore_index=True)

    date_columns_list: List = ['Modification_date', 'Creation_date']
    # Changing integer columns type (in sql) to DATETIME2 (for better decimal digit accuracy)
    date_time2_dict: Dict = {col_name: DATETIME2 for col_name in date_columns_list}
    updated_data_frame.to_sql('Files_Meta_Data', engine, if_exists='replace', dtype=date_time2_dict, schema='dbo')


def main():
    arguments = sys.argv[1:]
    root_directory, file_mapping_directory, apply_data_filters = [None] * 3  # Initial assignment
    try:
        root_directory = arguments[0]
        file_mapping_directory = arguments[1]
        apply_data_filters = arguments[2]
    except IndexError:
        if not len(arguments) ^ 0:  # If the user didn't specify any path
            root_directory = config.path
            file_mapping_directory = config.path
            apply_data_filters = False  # Default Value

    file_crawler(root_directory, file_mapping_directory, apply_data_filters)


if __name__ == '__main__':
    main()
