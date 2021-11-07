import pandas as pd

from sqlalchemy import create_engine, INTEGER
from sqlalchemy.engine import Engine
from database_updating import _fetching_sql_file_meta_data_table
from utils import calculate_md5_hash, extract_file_information, FILES_META_DATA_TABLE

"""
This module represents a system cache for all crawled files. It is responsible for maintaining an open engine connection
with the DB (reduces number of calls), and for the differential crawling and file handling, and also for updating the 
Meta data files table in the DB

Functions:

__init__() -- Callable within calling creating a class's attribute
__enter__() -- Executed when entering the scope after creating a class attribute
__exit__() -- Executed after exiting the scope in which the class's attribute was created
_connect() -- Opens a connection with the DB, and fetches the files Meta data table
_disconnect() -- Closes the connection with the DB, and Calls the updating Meta data table function
_clear() -- Drops the meta data table in DB and updates the existing meta data table to be an empty data frame
exists() -- Checks whether a file was already added to the DB before (for differentiability)
add_file() -- Adding a file to the Meta data table
_dump_existing() -- Updates the Meta data table in DB
"""


class FilesCache:
    _date_columns_list = ['Modification_date', 'Creation_date']
    engine: Engine
    conn: Engine
    existing_table: pd.DataFrame

    def __init__(self, connection_string: str) -> None:
        self.engine = create_engine(connection_string, echo=False)

    def __enter__(self):
        self._connect()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._disconnect()

    def _connect(self):
        self.conn = self.engine.connect()
        self.existing_table = _fetching_sql_file_meta_data_table(self.conn)

    def _disconnect(self):
        self._dump_existing()
        if self.conn and not self.conn.closed:
            self.conn.close()

    def _clear(self) -> None:
        try:
            self.conn.execute(f'DROP TABLE {FILES_META_DATA_TABLE};')
        except Exception as e:
            pass
        finally:
            # Initialize existing table to be an empty data frame
            self.existing_table = _fetching_sql_file_meta_data_table(self.conn)

    def exists(self, file_path: str) -> bool:
        file_name, modification_date, creation_date = extract_file_information(file_path)

        return ((self.existing_table['File_name'] == file_name) &
                (self.existing_table['File_path'] == file_path) &
                (self.existing_table['Modification_date'] == modification_date) &
                (self.existing_table['Creation_date'] == creation_date) &
                (self.existing_table['File_md5'] == calculate_md5_hash(file_path))).any()

    def add_file(self, file_path: str) -> None:
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

    def _dump_existing(self) -> None:
        # Changing integer columns type (in sql) to DATETIME2 (for better decimal digit accuracy)
        # date_time2_dict: Dict = {col_name: DATETIME2 for col_name in date_columns_list}
        # In sqlite we'll be changing column's date type to INTEGER (There's no DATETIME2 type)
        date_time2_dict = {col_name: INTEGER for col_name in self._date_columns_list}
        self.existing_table.to_sql(FILES_META_DATA_TABLE, self.conn, if_exists='replace', dtype=date_time2_dict)
