from typing import Any

import config
import sys

from file_crawler import crawl_file


def main():
    arguments = sys.argv[1:]
    # Initial assignment
    root_directory = None
    file_mapping_directory = None
    apply_data_filters = None
    try:
        root_directory = arguments[0]
        file_mapping_directory = arguments[1]
        apply_data_filters = arguments[2]
    except IndexError:
        if not arguments:  # If the user didn't specify any path
            root_directory = config.path
            file_mapping_directory = config.path
            apply_data_filters = False  # Default Value

    crawl_file(root_directory, file_mapping_directory, apply_data_filters)


if __name__ == '__main__':
    main()
