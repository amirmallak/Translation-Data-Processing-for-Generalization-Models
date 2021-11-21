import json
import config
import click

from typing import Dict
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
file_index_translate -- The json mapping file. Indicates which word (key) in the files fields (if it exists there) 
                        should be mapped (replaced) to which new word (value)

Functions:

crawl_file() -- The core function of the code. Crawls on all the differentiable files (by mean, files which hasn't been
                handled before - in previous code runs), cleans them, apply filters if specified, and translates (maps) 
                required fields into the corresponding one in the translation dictionary. It then creates a raw and 
                clean tables in the SQLite database and saves it there (concatenating tables in future runs), and in 
                addition creates a view table which in it presents the cleaned and translated (mapped) data fields
"""


def default_callback_builder(message):
    def inner():
        click.echo(message)

        return config.path

    return inner


@click.group()
def cli():
    pass


@cli.command(help="This command crawl all the files in the provided directory")
@click.option('--root_directory', default=default_callback_builder("Taking root dir from environment variable"))
@click.option('--file_mapping_directory', default=default_callback_builder("Taking files dir from environment variable"))
@click.option('--apply_data_filters', default=False)
def process_files(root_directory, file_mapping_directory, apply_data_filters):
    crawl_file(root_directory, file_mapping_directory, apply_data_filters)


@cli.command()
@click.option('--translation_dict_path', default=f'{config.path_mapping}\\oxford_dictionary_translation.json')
def create_mapping_dictionary(translation_dict_path):
    file_index_translate: Dict = {
        'Date_Different_Language': 'Date',
        'House_Different_Language': 'House',
        'Building_Different_Language': 'Building'
    }

    with open(translation_dict_path, 'w', encoding='utf-8') as json_file:
        json.dump(file_index_translate, json_file, ensure_ascii=False)


if __name__ == '__main__':
    cli()
