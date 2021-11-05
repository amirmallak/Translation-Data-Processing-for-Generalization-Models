import os

import pytest

import config

from pytest import fixture
from sqlalchemy import INTEGER, inspect
from files_cache import FilesCache
from utils import calculate_md5_hash, extract_file_information, FILES_META_DATA_TABLE

TEST_DB_NAME = 'database.db'


@fixture(scope='function')
def files_cache():
    fc = FilesCache(f'sqlite:///{TEST_DB_NAME}')
    fc.connect()
    assert not fc.existing_table.size, "Cache suppose to be empty"
    yield fc

    fc.clear()
    fc.disconnect()


def test_new_file_added_to_cache(files_cache: FilesCache):
    file_path = r'tests_files\Test_file_1.txt'
    files_cache.add_file(file_path)
    file_name, modification_date, creation_date = extract_file_information(file_path)
    file_md5 = calculate_md5_hash(file_path)
    expected_file_row = [file_name, file_path, modification_date, creation_date, file_md5]

    assert list(files_cache.existing_table.loc[0]) == expected_file_row, "Adding new file failed!"


def test_add_duplicate_file(files_cache: FilesCache):
    file_path = r'tests_files\Test_file_1.txt'
    files_cache.add_file(file_path)
    files_cache.add_file(file_path)
    file_name, modification_date, creation_date = extract_file_information(file_path)
    file_md5 = calculate_md5_hash(file_path)
    expected_file_row = [file_name, file_path, modification_date, creation_date, file_md5]

    assert list(files_cache.existing_table.loc[0]) == expected_file_row, "Adding new file failed!"
    assert files_cache.existing_table['File_name'].size == 1, "Duplicated files reduction doesn't work!"


def test_file_exist(files_cache: FilesCache):
    file_path = r'tests_files\Test_file_1.txt'
    files_cache.add_file(file_path)

    assert files_cache.exists(file_path), "Files cache exists function doesn't function properly"


def test_file_doesnt_exist(files_cache: FilesCache):
    file_path = r'tests_files\Test_file_1.txt'
    assert not files_cache.exists(file_path),\
        "Files cache exists function returned True when the files cache were supposed to be empty"

    files_cache.add_file(file_path)
    file_path_2 = r'tests_files\Test_csv_file.csv'
    assert not files_cache.exists(file_path_2), \
        "Files cache exists function returned True even though the the file wasn't added to the files cache"


def test_type_of_date(files_cache: FilesCache):
    insp = inspect(files_cache.engine)
    columns_table = insp.get_columns(FILES_META_DATA_TABLE)
    fields_names = ['Modification_date', 'Creation_date']
    for column in columns_table:
        if column['name'] in fields_names:
            assert isinstance(column['type'], INTEGER), f"sqlite table {column['name']} is not of type INTEGER!"


def test_database_exist():
    test_db_exist_name = 'test1.db'
    with FilesCache(f'sqlite:///{test_db_exist_name}') as fc:
        pass
    assert os.path.exists(test_db_exist_name), 'DB file not found'

    try:
        with FilesCache(f'sqlite:///{test_db_exist_name}') as fc:
            pass
    except Exception as e:
        pytest.fail('Using existing database raises an error')
    finally:
        os.remove(test_db_exist_name)


def test_database_doesnt_exist():
    test_db_not_exist_name = 'test2.db'
    assert not os.path.exists(test_db_not_exist_name), 'DB file found when not supposed to!'

    try:
        with FilesCache(f'sqlite:///{test_db_not_exist_name}') as fc:
            pass
        assert os.path.exists(test_db_not_exist_name), 'DB file not created'
    finally:
        os.remove(test_db_not_exist_name)

