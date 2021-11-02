import hashlib
import json
import os
import pathlib
from datetime import datetime
from functools import lru_cache

import config

from typing import Dict, Optional, Tuple

FILES_META_DATA_TABLE = 'Files_Meta_Data'


def extract_file_information(file_path: str) -> Tuple[str, datetime, datetime]:
    file_name, _ = os.path.splitext(file_path.split('\\')[-1])
    file_information = pathlib.Path(file_path)  # All file's information
    modification_time = file_information.stat().st_mtime  # Modification time (in [sec])
    modification_date = datetime.fromtimestamp(modification_time)  # Modification Date (in date view)
    creation_time = file_information.stat().st_ctime  # Creation time (in [sec])
    creation_date = datetime.fromtimestamp(creation_time)  # Creation Date (in date view)

    return file_name, modification_date, creation_date


def merge_dictionaries(dict1, dict2) -> Dict:
    merged_dictionary: Dict = {**dict1, **dict2}

    return merged_dictionary


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


@lru_cache(maxsize=int((1e3 + 2e1 + 3e0)))
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


def create_engine_path() -> str:
    engine_path: str = f'{config.sql_server}+{config.accessing_library}://{config.mssql_username}:{config.password}' \
                       f'@{config.server_name}:{config.server_conn_port}/{config.db_name}?driver={config.driver_name}'
    return engine_path
