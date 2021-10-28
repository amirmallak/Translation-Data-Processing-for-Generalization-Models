from typing import Dict
import json


if __name__ == '__main__':

    file_index_translate: Dict = {
        'Date_Different_Language': 'Date',
        'House_Different_Language': 'House',
        'Building_Different_Language': 'Building'
    }

    with open(r'C:\Users\Ameer\Documents\Python_Projects\Technological_Unit'
              r'\Oxford_Dictionary_Translation.json', 'w', encoding='utf-8') as json_file:
        json.dump(file_index_translate, json_file, ensure_ascii=False)
