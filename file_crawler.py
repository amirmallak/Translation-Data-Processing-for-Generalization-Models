from typing import List


def get_extensions():
    csv_extensions: Dict = dict.fromkeys(['.csv'], pd.read_csv)
    excel_extensions: Dict = dict.fromkeys(['.xlxs', 'xlsm', '.xlsb', '.xltx', 'xltm', 'xls', '.xlt', '.xml',
                                            '.xlam', '.xla', '.xlw', '.xlr'], pd.read_excel)
    extensions: Dict = {**csv_extensions, **excel_extensions}

    return extensions


def crawl_file(root_directory: str,
               file_mapping_directory: str,
               apply_data_filters: bool,
               update_db: bool = False) -> None:
    list_csv_files: List[str] = []
    extension_types = get_extensions()
    file_names_list = []
    file_path_list = []
    modification_date_list = []
    creation_date_list = []
    file_md5_list = []

    # Fetching SQL File_Meta_Data Table
    # engine_path = create_engine_path()
    engine_path = config.connection_string
    engine = create_engine(engine_path, echo=False)
    existing_table = fetching_sql_file_meta_data_table(engine)

    # Fetching the all file's columns translation dictionary
    translate_index_file_path = translation_dictionary_path(file_mapping_directory)
    translate_dict: Dict = read_json_translation_file(translate_index_file_path)

    for dir_name, sub_dir_list, file_List in os.walk(root_directory):
        for file in file_List:
            file_name, extension = os.path.splitext(file)
            extension = extension.lower()
            if (extension in extension_types.keys()) and (file_name[0:2] != '~$'):  # If extension is supported and
                # file is not open
                # Fetching the file's columns translation dictionary
                # Translate_index_file_path = file_translation_dictionary_path(file_mapping_directory, file, file_name)

                file_path = os.path.join(dir_name, file)

                file_information = pathlib.Path(file_path)  # All file's information
                modification_time = file_information.stat().st_mtime  # Modification time (in [sec])
                modification_date = datetime.fromtimestamp(modification_time)  # Modification Date (in date view)
                creation_time = file_information.stat().st_ctime  # Creation time (in [sec])
                creation_date = datetime.fromtimestamp(creation_time)  # Creation Date (in date view)

                try:
                    if ((existing_table['File_name'] == file_name) &
                        (existing_table['File_path'] == file_path) &
                        (existing_table['Modification_date'] == modification_date) &
                        (existing_table['Creation_date'] == creation_date)).any():
                        continue
                except TypeError as e:
                    pass

                file_md5_hash = calculate_md5_hash(file_path)
                try:
                    if (existing_table['File_md5'] == file_md5_hash).any():
                        continue
                except TypeError as e:
                    pass

                file_names_list.append(file_name)
                file_path_list.append(file_path)
                modification_date_list.append(modification_date)
                creation_date_list.append(creation_date)
                file_md5_list.append(file_md5_hash)

                file_name_list, data_frame_list = create_data_frames(file_path, extension_types[extension])

                if apply_data_filters:
                    data_frame_list = data_filtering(data_frame_list)  # TODO: Change to be choosable

                if update_db:
                    update_database(data_frame_list, file_name_list)
                    create_sql_view_tables(translate_dict)

                list_csv_files.append(file_path)

    file_information_data_frame = pd.DataFrame({'File_name': file_names_list,
                                                'File_path': file_path_list,
                                                'Modification_date': modification_date_list,
                                                'Creation_date': creation_date_list,
                                                'File_md5': file_md5_list})
    updated_data_frame = pd.concat([existing_table, file_information_data_frame], ignore_index=True)
    updated_data_frame.drop_duplicates(inplace=True, ignore_index=True)

    date_columns_list: List = ['Modification_date', 'Creation_date']
    # Changing integer columns type (in sql) to DATETIME2 (for better decimal digit accuracy)
    # date_time2_dict: Dict = {col_name: DATETIME2 for col_name in date_columns_list}
    date_time2_dict: Dict = {col_name: INTEGER for col_name in date_columns_list}
    updated_data_frame.to_sql(FILES_META_DATA, engine, if_exists='replace', dtype=date_time2_dict)
