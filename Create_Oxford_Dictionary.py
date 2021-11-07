import json
import config

from typing import Dict

"""
This module creates a json translation (mapping) file by the name oxford_dictionary_translation.

Dynamic objects:

file_index_translate -- The json mapping file. Indicates which word (key) in the files fields (if it exists there) 
                        should be mapped (replaced) to which new word (value)
"""


def main():
    file_index_translate: Dict = {
        'Date_Different_Language': 'Date',
        'House_Different_Language': 'House',
        'Building_Different_Language': 'Building'
    }

    with open(f'{config.path_mapping}\\oxford_dictionary_translation.json', 'w', encoding='utf-8') as json_file:
        json.dump(file_index_translate, json_file, ensure_ascii=False)


if __name__ == '__main__':
    main()
