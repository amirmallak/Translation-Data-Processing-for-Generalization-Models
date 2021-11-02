from pytest import fixture
from files_cache import FilesCache


@fixture(scope='function')
def files_cache():
    fc = FilesCache()
    fc.connect()
    fc.add_file(r'tests\tests_files\Test_file_1.txt')
    yield fc

    fc.disconnect()


def test_new_file_added_to_cache():
    pass


def test_add_duplicate_file():
    pass


def test_file_exist():
    pass


def test_file_exist_by_md5():
    pass


def test_file_doesnt_exist():
    pass


def test_file_doesnt_exist_by_md5():
    pass


def test_type_of_date():
    pass


def test_database_exist():
    pass


def test_database_doesnt_exist():
    pass
