import os
import pathlib
from datetime import datetime
from typing import Any
import pandas as pd
from sqlalchemy import create_engine, INTEGER
from sqlalchemy.engine import Engine

import config
from database_updating import fetching_sql_file_meta_data_table
from utils import calculate_md5_hash, extract_file_information, FILES_META_DATA_TABLE


class FilesCache:
    _date_columns_list = ['Modification_date', 'Creation_date']
    engine: Engine
    existing_table: pd.DataFrame

    def __init__(self):
        engine_path = config.connection_string
        self.engine = create_engine(engine_path, echo=False)

    def __enter__(self):
        self.conn = self.engine.connect()
        self.existing_table = fetching_sql_file_meta_data_table(self.conn)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._dump_existing()
        if self.conn and not self.conn.closed:
            self.conn.close()

    def exists(self, file_path):
        file_name, modification_date, creation_date = extract_file_information(file_path)

        return ((self.existing_table['File_name'] == file_name) &
                (self.existing_table['File_path'] == file_path) &
                (self.existing_table['Modification_date'] == modification_date) &
                (self.existing_table['Creation_date'] == creation_date) &
                self.existing_table['File_md5'] == calculate_md5_hash(file_path)).any()

    def add_file(self, file_path):
        file_name, modification_date, creation_date = extract_file_information(file_path)
        file_md5 = calculate_md5_hash(file_path)
        self.existing_table = self.existing_table.append(
            {'File_name': file_name,
             'File_path': file_path,
             'Modification_date': modification_date,
             'Creation_date': creation_date,
             'File_md5': file_md5},
            ignore_index=True)
        self.existing_table.drop_duplicates(inplace=True, ignore_index=True)

    def _dump_existing(self):
        # Changing integer columns type (in sql) to DATETIME2 (for better decimal digit accuracy)
        # date_time2_dict: Dict = {col_name: DATETIME2 for col_name in date_columns_list}
        date_time2_dict = {col_name: INTEGER for col_name in self._date_columns_list}
        self.existing_table.to_sql(FILES_META_DATA_TABLE, self.conn, if_exists='replace', dtype=date_time2_dict)
