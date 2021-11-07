import os
import pandas as pd
import config

from typing import Dict
from data_processing import create_data_frames, data_filtering
from database_updating import update_database, _create_sql_view_tables
from files_cache import FilesCache
from utils import translation_dictionary_path, read_json_translation_file

"""
This module is the program's core module. It crawls over the files in a differential manner, filters any necessary 
data frames, creates relevant tables and adds them to the DB.

Functions:

_get_extensions() -- Formats a combined dictionary for file types corresponding to its a callback function
crawl_file() -- Crawls over all directory hierarchical files, extract relevant information, filter the files, and builds
                relevant tables and adds them to the DB
"""


def _get_extensions() -> Dict:
    csv_extensions: Dict = dict.fromkeys(['.csv'], pd.read_csv)
    excel_extensions: Dict = dict.fromkeys(['.xlxs', 'xlsm', '.xlsb', '.xltx', 'xltm', 'xls', '.xlt', '.xml',
                                            '.xlam', '.xla', '.xlw', '.xlr'], pd.read_excel)
    extensions: Dict = {**csv_extensions, **excel_extensions}

    return extensions


def crawl_file(root_directory: str,
               file_mapping_directory: str,
               apply_data_filters: bool,
               update_db: bool = False) -> None:

    extension_types = _get_extensions()

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
                        _create_sql_view_tables(translate_dict)

                    files_cache.add_file(file_path)
