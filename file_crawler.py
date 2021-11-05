import os
import pathlib
import pandas as pd
import config

from datetime import datetime
from typing import List, Dict
from sqlalchemy import create_engine, INTEGER
from data_processing import create_data_frames, data_filtering
from database_updating import fetching_sql_file_meta_data_table, update_database, create_sql_view_tables
from files_cache import FilesCache
from utils import translation_dictionary_path, read_json_translation_file, calculate_md5_hash


def get_extensions():
    csv_extensions: Dict = dict.fromkeys(['.csv'], pd.read_csv)
    excel_extensions: Dict = dict.fromkeys(['.xlxs', 'xlsm', '.xlsb', '.xltx', 'xltm', 'xls', '.xlt', '.xml',
                                            '.xlam', '.xla', '.xlw', '.xlr'], pd.read_excel)
    extensions: Dict = {**csv_extensions, **excel_extensions}

    return extensions


def crawl_file(root_directory: str,
               file_mapping_directory: str,
               apply_data_filters: bool,
               update_db: bool = False, FILES_META_DATA=None) -> None:

    extension_types = get_extensions()

    # Fetching the all file's columns translation dictionary
    translate_index_file_path = translation_dictionary_path(file_mapping_directory)
    translate_dict: Dict = read_json_translation_file(translate_index_file_path)

    with FilesCache(config.connection_string) as files_cache:
        for dir_name, sub_dir_list, file_List in os.walk(root_directory):
            for file in file_List:
                file_name, extension = os.path.splitext(file)
                extension = extension.lower()

                # If extension is supported and file is not open
                if (extension in extension_types.keys()) and (file_name[0:2] != '~$'):
                    file_path = os.path.join(dir_name, file)

                    if files_cache.exists(file_path):
                        continue

                    file_name_list, data_frame_list = create_data_frames(file_path, extension_types[extension])

                    if apply_data_filters:
                        data_frame_list = data_filtering(data_frame_list)

                    if update_db:
                        update_database(data_frame_list, file_name_list)
                        create_sql_view_tables(translate_dict)

                    files_cache.add_file(file_path)
