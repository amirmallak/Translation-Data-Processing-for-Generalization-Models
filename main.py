import config
import sys

from typing import Optional
from file_crawler import crawl_file


"""
This module accepts arguments from the user and runs the entire code.
The user needs to enter three arguments,
1. The root directory path in which the files that needs to be handled exists in.
2. The file mapping directory path in which the translation json file could be found.
3. A boolean string which indicates whether to apply some filters on the cleaned data.

If the user didn't specify any arguments, a default paths for the root directory and for the file mapping directory is 
taken from the config file. Applying filters won't happen in this case.

Dynamic objects:

root_directory -- A path which contains the files that needs to be handled
file_mapping_directory -- A path which contains a translation mapping json file (in order to switch the header fields
                          in the desired files into the desired word "description" in the json mapping file) 
apply_data_filters -- A boolean parameter which indicates whether to apply a set of filters on the raw data in the 
                      pre-processing phase after cleaning it

Functions:

crawl_file() -- The core function of the code. Crawls on all the differentiable files (by mean, files which hasn't been
                handled before - in previous code runs), cleans them, apply filters if specified, and translates (maps) 
                required fields into the corresponding one in the translation dictionary. It then creates a raw and 
                clean tables in the SQLite database and saves it there (concatenating tables in future runs), and in 
                addition creates a view table which in it presents the cleaned and translated (mapped) data fields
"""


def main():
    """
    Input: None
    :return: None
    """
    arguments = sys.argv[1:]
    # Initial assignment
    root_directory: Optional[str] = None
    file_mapping_directory: Optional[str] = None
    apply_data_filters: Optional[bool] = None
    try:
        root_directory = arguments[0]
        file_mapping_directory = arguments[1]
        apply_data_filters = bool(arguments[2])
    except IndexError:
        if not arguments:  # If the user didn't specify any path
            root_directory = config.path
            file_mapping_directory = config.path_mapping
            apply_data_filters = False  # Default Value

    crawl_file(root_directory, file_mapping_directory, apply_data_filters)


if __name__ == '__main__':
    main()
