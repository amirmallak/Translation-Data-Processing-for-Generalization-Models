import numpy as np
import pandas as pd
import config

from sqlalchemy.engine import Engine
from typing import Optional, List, Dict, Tuple
from sqlalchemy import NVARCHAR, create_engine
from data_processing import _pandas_to_numeric, _create_clean_data_frame
from utils import create_engine_path, FILES_META_DATA_TABLE

"""
This module is responsible for the interface with the Database. Fetching, Updating, and Creating different DB Tables and 
DB views happens here. It's also responsible for the communication, and translation of a new table according to the 
mapping dictionary.

Functions:

_fetch_table() -- Fetches a required table from DB
_data_frames_formatting() -- Updates an existing table in DB with a new one. If there's no matching table in DB yet, it
                             creates a new one
_add_to_db() -- Adds tables to DB with corresponding type
update_database() -- Iterate throw each differential data frame and adds it to DB
_fetching_sql_file_meta_data_table() -- Fetches the Meta Data Table from the DB. If it doesn't exist, creates an empty 
                                        one with the desired fields
_create_view_sql_query() -- Creates an SQL View query (for raw and clean view tables)
_is_relevant_table() -- Checks if the table is relevant for translation/mapping
_create_sql_view_tables() -- Creates an SQL View Table in the database
"""


def _fetch_table(table_name: str, engine: Engine) -> pd.DataFrame:
    try:  # Check if the table {table_name} already exists
        existing_table: Optional[pd.DataFrame] = pd.read_sql(f'SELECT * FROM [raw].[{table_name}];', engine)
        existing_table.drop(columns=[existing_table.columns[0]], inplace=True)  # Dropping the SQL's 'index' column
    except BaseException as exc:  # If this is the first insertion
        print(f'\nAn {exc} Has Occurred!\n')
        print(f'There wasn\'t a SQL Table by name: {table_name}!\n')
        print(f'Creating new one...\n')
        existing_table = None

    return existing_table


def _data_frames_formatting(data_frame_list: List[pd.DataFrame], table_name_list: List[str], engine: Engine) -> \
                           (pd.DataFrame, pd.DataFrame, str):

    for table_name, data_frame in zip(table_name_list, data_frame_list):

        existing_table = _fetch_table(table_name, engine)

        # Take table from mssql, concat with data_frame (ignore_index=True), drop duplicates, send again to sql
        """
        IMPORTANT:
        We're reading the SQL table to the local machine RAM (as Panda's DataFrame), concatenating it (if exists)
        with the current data_frame, and then sending it back to mssql.

        Another option was to insert the current data_frame as Upserting (Insert-Update) Panda's to SQL command.
        While it's more practical, faster, reducing usage of memory (RAM), and back_forth process saving. In case
        there's a current data_frame with different column names than the existing one (Table) in mssql, it wouldn't
        work! (the insertion command) - SQL doesn't know how to update table's columns.

        For this reason, it was decided to use Panda's 'Concat' command (which required reading the SQL Table,
        concatenating it locally, and then updating (dropping) the existed one on mssql).
        """
        concatenated_table = pd.concat([existing_table, data_frame], ignore_index=True)  # Reordering the rows indexes
        concatenated_table.replace(0, '0', inplace=True)
        concatenated_table.fillna('np.nan', inplace=True)
        concatenated_table = _pandas_to_numeric(concatenated_table)
        concatenated_table.drop_duplicates(inplace=True, ignore_index=True)
        concatenated_table.replace('np.nan', np.nan, inplace=True)

        # Create clean DataFrame
        clean_data_frame = _create_clean_data_frame(concatenated_table)

        yield concatenated_table, clean_data_frame, table_name


def _add_to_db(raw_table: pd.DataFrame, clean_table: pd.DataFrame, table_name: str, engine: Engine) -> None:
    # Changing non-integer columns type (in sql) to NVarChar, and integer ones to NUMERIC
    # (for future interpolation on data)
    nvarchar_dict: Dict = {col_name: NVARCHAR for col_name in raw_table.columns}

    raw_table.to_sql(table_name, engine, if_exists='replace', dtype=nvarchar_dict, schema='Raw')

    clean_table.to_sql(table_name, engine, if_exists='replace', dtype=nvarchar_dict, schema='Clean')


def update_database(data_frame_list: List[pd.DataFrame], file_name_list: List[str]) -> None:
    engine_path: str = create_engine_path()
    engine = create_engine(engine_path, echo=False)

    for raw_table, clean_table, table_name in _data_frames_formatting(data_frame_list, file_name_list, engine):
        _add_to_db(raw_table, clean_table, table_name, engine)


def _fetching_sql_file_meta_data_table(connection):
    # Fetching SQL File_Meta_Data Table
    try:  # Check if the table 'Meta_Data' already exists
        existing_table: Optional[pd.DataFrame] = pd.read_sql(f'SELECT * FROM [{FILES_META_DATA_TABLE}];', connection)
        existing_table.drop(columns=[existing_table.columns[0]],
                            inplace=True)  # Dropping the SQL's 'index' column
    except Exception as e:  # If this is the first insertion
        print(f'\nAn {e} has Occurred!\n')
        print(f'There wasn\'t an SQL Table by name: Meta_Data!\n')
        print(f'Creating one...\n')
        existing_table = pd.DataFrame({'File_name': [],
                                       'File_path': [],
                                       'Modification_date': [],
                                       'Creation_date': [],
                                       'File_md5': []})

    return existing_table


def _create_view_sql_query(view_table_name: str, translate_dict: Dict, table_name: str,
                           sql_columns_names: Optional[List[Tuple]]) -> (str, str):
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


def _is_relevant_table(translate_dict: Dict, sql_columns_names: Optional[List[Tuple]]) -> bool:
    relevant_table: bool = False
    for column in sql_columns_names:
        if translate_dict.get(column[0], None):
            relevant_table = True
            break
    return relevant_table


def _create_sql_view_tables(translate_dict: Dict) -> None:
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
            relevant_table: bool = _is_relevant_table(translate_dict, sql_columns_names)
            if relevant_table:
                view_table_name = table_name.replace(' ', '_')

                raw_view_query, clean_view_query = _create_view_sql_query(view_table_name, translate_dict, table_name,
                                                                          sql_columns_names)

                # Dropping the View table if it has been already created (to be able to insert a new one)
                conn.execute(f'Drop View if exists [raw].[V_{view_table_name}]')
                conn.execute(f'Drop View if exists [clean].[V_{view_table_name}]')
                # Creating a new View table
                conn.execute(raw_view_query)
                conn.execute(clean_view_query)
