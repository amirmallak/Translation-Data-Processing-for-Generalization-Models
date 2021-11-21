from dotenv import load_dotenv
from os import getenv

"""
This module is a configuration module. It saves (maps) all the needed default values for certain parameters, and configs
required configurations for connecting to the desired DB.
"""

load_dotenv()  # A function for handling a .env file with the necessary configurations

path = getenv('Root_Directory', None)
sql_server = getenv('SQL_Server', None)
accessing_library = getenv('ACCESS_LIBRARY', None)
mssql_username = getenv('MSSQL_USERNAME', None)
password = getenv('PASSWORD', None)
server_name = getenv('SERVER_NAME', None)
server_conn_port = getenv('SERVER_CONN_PORT', None)
db_name = getenv('DB_NAME', None)
driver_name = getenv('DRIVER_NAME', None)

# Table Names
default_table_name = getenv('DEFAULT_TABLE_NAME', None)
type2_table_name = getenv('TYPE2_TABLE_NAME', None)
test_table_name = getenv('TEST_TABLE_NAME', None)

# Alternative Directory Path
alternative_path = getenv('Alternative_PATH', None)

# Mapping Files Path
path_mapping = getenv('PATH_MAPPING', None)

# Connection String
connection_string = getenv('CONN_STR', None)
