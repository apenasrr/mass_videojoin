"""
    Create by: apenasrr
    Source: https://github.com/apenasrr/mass_videojoin

    A smart tool to optimize and make turbo join in a massive video collection
"""

import os
import pandas as pd
import datetime
import logging
from video_tools import change_width_height_mp4, get_video_details, \
    join_mp4, split_mp4
from config_handler import handle_config_file
import unidecode
import natsort
import glob
import sys
import hashlib


def logging_config():

    logfilename = 'log-' + 'mass_videojoin' + '.txt'
    logging.basicConfig(filename=logfilename, level=logging.DEBUG,
                        format=' %(asctime)s-%(levelname)s-%(message)s')
    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter(' %(asctime)s-%(levelname)s-%(message)s')
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    logger = logging.getLogger(__name__)


def clean_cmd():

    def clear(): return os.system('cls')
    clear()


def df_sort_human(df):
    """
    Sort files and folders in human way. 
    So after folder/file '1' comes '2', instead of '10' and '11'.
    Simple yet flexible natural sorting in Python.
    When you try to sort a list of strings that contain numbers, 
    the normal python sort algorithm sorts lexicographically, 
    so you might not get the results that you expect:
    More info: https://github.com/SethMMorton/natsort

    :input: DataFrame. With columns [file_folder, file_name]
    :return: DataFrame. Sort in a human way by [file_folder, file_name]
    """

    def sort_human(list_):

        list_ = natsort.natsorted(list_)

        return list_

    def sort_df_column_from_list(df, column_name, sorter):
        """
        :input: df: DataFrame
        :input: column_name: String
        :input: sorter: List
        :return: DataFrame
        """

        sorterIndex = dict(zip(sorter, range(len(sorter))))
        df['order'] = df[column_name].map(sorterIndex)
        df.sort_values(['order'], ascending=[True], inplace=True)
        df.drop(['order', column_name], 1, inplace=True)

        return df

    column_name = 'path_file'
    df[column_name] = df['file_folder'] + '\\' + df['file_name']
    list_path_file = df[column_name].tolist()
    sorter = sort_human(list_path_file)
    df = sort_df_column_from_list(df, column_name, sorter)

    return df


def gen_report(path_dir):

    # TODO input more file video extension:
    # https://dotwhat.net/type/video-movie-files

    tuple_video_extension = (".mp4", ".avi", ".webm", '.ts', '.vob',
                             '.mov', '.mkv', '.wmv')
    str_tuple_video_extension = ', '.join(tuple_video_extension)
    logging.info(f'Find for video with extension: {str_tuple_video_extension}')
    l = []
    for root, dirs, files in os.walk(path_dir):

        for file in files:
            file_lower = file.lower()
            if file_lower.endswith(tuple_video_extension):
                logging.info(f'Selected file: {file}')

                path_file = os.path.join(root, file)
                dict_inf = get_video_details(path_file)
                (mode, ino, dev, nlink, uid,
                 gid, size, atime, mtime, ctime) = os.stat(path_file)
                mtime = datetime.datetime.fromtimestamp(mtime)

                d = {}
                d['mtime'] = mtime
                ctime = datetime.datetime.fromtimestamp(ctime)
                d['creation_time'] = ctime
                d['file_folder'] = root
                d['file_name'] = file
                d['file_size'] = os.path.getsize(path_file)

                try:
                    d['duration'] = dict_inf['duration']
                except:
                    logging.error(f'Video without duration:\n{path_file}\n' +
                                  'Please check and delete the file if ' +
                                  'necessary')

                    d['duration'] = ''
                    continue
                d['bitrate'] = dict_inf['bitrate']
                try:
                    d['video_codec'] = dict_inf['video']['codec']
                except:
                    logging.error('File above dont have tag "video" in ' +
                                  f'detail file:\n{file}')
                    continue
                d['video_profile'] = dict_inf['video']['profile']
                d['video_resolution'] = dict_inf['video']['resolution']
                d['video_bitrate'] = dict_inf['video']['bitrate']

                # some videos dont have audio
                try:
                    d['audio_codec'] = dict_inf['audio']['codec']
                    d['audio_frequency'] = dict_inf['audio']['frequency']
                    d['audio_bitrate'] = dict_inf['audio']['bitrate']
                except:
                    d['audio_codec'] = ''
                    d['audio_frequency'] = ''
                    d['audio_bitrate'] = ''
                d['video_resolution_to_change'] = ''
                l.append(d)
            else:
                logging.info(f'Unselected file: {file}')
    df = pd.DataFrame(l)

    return df


def get_video_details_with_group(df):

    df['key_join_checker'] = df['audio_codec'] + '-' + \
        df['video_codec'] + '-' + \
        df['video_resolution']

    # set group_encode
    df['group_encode'] = 1
    for index, row in df.iterrows():
        if index > 0:
            group_encode_value_prev = df.loc[index-1, 'group_encode']
            if row['key_join_checker'] != df.loc[index-1, 'key_join_checker']:
                df.loc[index, 'group_encode'] = group_encode_value_prev + 1
            else:
                df.loc[index, 'group_encode'] = group_encode_value_prev

    return df


def get_list_chunk_videos_from_group(df, group_no, max_size_mb):

    max_size_bytes = max_size_mb * 1024**2
    mask = df['group_encode'].isin([group_no])

    df['file_path'] = df['file_folder'] + '\\' + \
        df['file_name']

    df_group = df.loc[mask, :]
    list_chunk_videos = []
    chunk_size = 0
    list_videos = []
    for index, row in df_group.iterrows():
        if chunk_size + row['file_size'] > max_size_bytes:
            logging.info(f'join video from {len(list_videos)} files')
            list_chunk_videos.append(list_videos)

            list_videos = []
            chunk_size = 0

        list_videos.append(row['file_path'])
        chunk_size += row['file_size']

    if len(list_videos) > 0:
        logging.info(f'join video from {len(list_videos)} files')
        list_chunk_videos.append(list_videos)
        list_videos = []

    logging.info(f'group {group_no} will generate ' +
                 f'{len(list_chunk_videos)} videos')
    return list_chunk_videos


def get_list_chunk_videos(df, max_size_mb):

    list_group = df['group_encode'].unique().tolist()
    list_final = []

    for group_no in list_group:
        group_no = str(group_no)
        list_chunk_videos = get_list_chunk_videos_from_group(df, group_no,
                                                             max_size_mb)
        list_final += list_chunk_videos
        print('')

    return list_final


def get_name_dir_origin():

    name_file_folder_name = get_txt_folder_origin()
    dir_name_saved = get_txt_content(name_file_folder_name)

    return dir_name_saved


def get_path_folder_output_video():

    path_folder_output = get_name_dir_origin()
    folder_name = 'output_' + path_folder_output
    ensure_folder_existence([folder_name])

    path_folder_output_video = os.path.join(folder_name, 'output_videos')
    ensure_folder_existence([path_folder_output_video])

    return path_folder_output_video


def join_videos(df, max_size_mb, start_index_output):

    # path_folder_output = userpref_folderoutput()
    path_folder_output = get_path_folder_output_video()

    # default_filename_output = input('Enter a default name for the joined ' +\
    # 'videos: ')
    default_filename_output = get_name_dir_origin()

    df['file_path'] = df['file_folder'] + '\\' + df['file_name']
    list_chunk_videos = get_list_chunk_videos(df, max_size_mb)
    df['file_output'] = ''

    for index, list_file_path in enumerate(list_chunk_videos):
        file_count = index + start_index_output
        file_name_output = f'{default_filename_output}-%03d.mp4' % file_count
        file_path_output = os.path.join(path_folder_output, file_name_output)
        join_mp4(list_file_path, file_path_output)

        # register file_output in dataframe
        mask_files_joined = df['file_path'].isin(list_file_path)
        df.loc[mask_files_joined, 'file_output'] = file_name_output

    print(f'total: {len(list_chunk_videos)} videos')

    return df


def exclude_all_files_from_folder(path_folder):

    path_folder_regex = os.path.join(path_folder, '*')
    r = glob.glob(path_folder_regex)
    for i in r:
        os.remove(i)


def make_reencode(path_file_report):

    def get_file_name_dest(file_folder_origin, file_name_origin):
        """
        Create a hashed file name dest. 
        Template: reencode_{file_name_origin}_{hash}.mp4"
        """
        file_folder_origin_encode = file_folder_origin.encode('utf-8')
        hash = hashlib.md5(file_folder_origin_encode).hexdigest()[:5]
        file_name_origin_without_extension = \
            os.path.splitext(file_name_origin)[0]
        file_name_dest = 'reencode_' + \
                         file_name_origin_without_extension + '_' + \
                         hash + '.mp4'
        return file_name_dest

    def create_backup_columns(df):

        # Ensure creation of column bellow
        list_backup_columns = ('file_folder', 'file_name', 'file_size',
                               'video_resolution')

        for column_name in list_backup_columns:
            new_column_name = column_name + '_origin'
            if new_column_name not in df.columns:
                df[new_column_name] = df[column_name]
        return df

    def get_next_video_to_reencode(path_file_report):

        try:
            df = pd.read_excel(path_file_report, engine='openpyxl')
        except Exception as e:
            logging.error(f"Can't open file: {path_file_report}")
            logging.error(e)
        mask_df_to_reencode = ~df['video_resolution_to_change'].isna()
        mask_df_reencode_not_done = df['reencode_done'].isin([0])
        mask_df_to_reencode = mask_df_to_reencode & mask_df_reencode_not_done

        df_to_reencode = df.loc[mask_df_to_reencode, :]

        qt_videos_to_reencode = df_to_reencode.shape[0]
        if qt_videos_to_reencode == 0:
            return False

        df_to_reencode = df_to_reencode.reset_index(drop=True)
        dict_first_line = df_to_reencode.loc[0, :]
        return dict_first_line

    def reencode_video(dict_):

        try:
            video_resolution_to_change = dict_['video_resolution_to_change']
            size_width, size_height = \
                video_resolution_to_change.split('x')
        except Exception as e:
            path_file_origin = os.path.join(dict_['file_folder_origin'],
                                            dict_['file_name_origin'])
            logging.error('Parse. Column video_resolution_to_change: ' +
                          f'"{video_resolution_to_change}". '
                          f'File:\n{path_file_origin}')
            return False

        file_folder_origin = dict_['file_folder_origin']
        file_name_origin = dict_['file_name_origin']
        path_file_origin = os.path.join(file_folder_origin,
                                        file_name_origin)

        file_name_dest = get_file_name_dest(file_folder_origin,
                                            file_name_origin)

        path_folder_dest = os.path.join(folder_script_path, 'videos_encoded')
        path_file_dest = os.path.join(path_folder_dest,
                                      file_name_dest)

        # Make video reencode
        logging.info(f'Start reencode: {path_file_origin}')

        change_width_height_mp4(path_file_origin, size_height,
                                size_width, path_file_dest)

    def ask_for_delete_old_videos_encode(path_folder_encoded):

        for root, dirs, files in os.walk(path_folder_encoded):

            list_file_name_encoded = list(files)

        len_list_file_name_encoded = len(list_file_name_encoded)
        if len_list_file_name_encoded > 0:
            print('\nThere is files in videos_encoded folder.\n' +
                  'Do you wish delete them?')
            answer_delete = input('(None for yes) Answer: ')

            if answer_delete == '':
                confirm_delete = input('\nType Enter to delete all ' +
                                       'video_encoded files.')
                if confirm_delete == '':
                    exclude_all_files_from_folder(path_folder_encoded)
                else:
                    pass

    def update_file_report(path_file_report, dict_video_data):

        try:
            df = pd.read_excel(path_file_report, engine='openpyxl')
        except Exception as e:
            logging.error(f"Can't open file: {path_file_report}")
            logging.error(e)

        # find path_folder_dest and path_file_dest
        file_folder_origin = dict_video_data['file_folder_origin']
        file_name_origin = dict_video_data['file_name_origin']
        path_file_origin = os.path.join(file_folder_origin,
                                        file_name_origin)
        file_name_dest = get_file_name_dest(file_folder_origin,
                                            file_name_origin)
        path_folder_dest = os.path.join(folder_script_path,
                                        'videos_encoded')
        path_file_dest = os.path.join(path_folder_dest,
                                      file_name_dest)

        # Check if file_name_dest exist
        test_file_exist = os.path.isfile(path_file_dest)
        if test_file_exist is False:
            logging.error('After reencode, when update, ' +
                          f'reencoded file not exist:\n{path_file_dest}')
            sys.exit()

        # locate index video in df
        mask_file_folder = df['file_folder'].isin([file_folder_origin])
        mask_file_name = df['file_name_origin'].isin([file_name_origin])
        mask_line = mask_file_folder & mask_file_name
        df_filter = df.loc[mask_line, :]
        len_df_filter = len(df_filter)
        if len_df_filter != 1:
            logging.error(f'Need 1. Find {len_df_filter} line for ' +
                          f'video: {path_file_origin}')
            sys.exit()
        index_video = df_filter.index

        # update df
        df.loc[index_video, 'file_folder'] = \
            os.path.abspath(path_folder_dest)
        df.loc[index_video, 'file_name'] = file_name_dest

        file_size = os.stat(path_file_dest).st_size
        video_resolution_to_change = \
            dict_video_data['video_resolution_to_change']

        df.loc[index_video, 'file_size'] = file_size
        df.loc[index_video, 'video_resolution'] = \
            video_resolution_to_change

        # from encoded video get video metadata
        metadata = get_video_details(path_file_dest)
        # register video metadata
        df.loc[index_video, 'bitrate'] = metadata['bitrate']
        df.loc[index_video, 'video_bitrate'] = metadata['video']['bitrate']
        df.loc[index_video, 'video_codec'] = metadata['video']['codec']
        df.loc[index_video, 'audio_codec'] = metadata['audio']['codec']
        df.loc[index_video, 'audio_bitrate'] = metadata['audio']['bitrate']
        df.loc[index_video, 'duration'] = metadata['duration']
        df.loc[index_video, 'reencode_done'] = 1
        return df

    folder_script_path = get_folder_script_path()
    path_folder_encoded = os.path.join(folder_script_path, 'videos_encoded')

    df = pd.read_excel(path_file_report, engine='openpyxl')
    df = create_backup_columns(df)

    ask_for_delete_old_videos_encode(path_folder_encoded)

    # TODO Test if all files mark as '1' in column reencode_done,
    # TODO  are in folder videos_encoded

    # Ensure creation of column 'reencode_done'. Pseudobolean 1 or 0
    if 'reencode_done' not in df.columns:
        df['reencode_done'] = 0

    # Save reports
    df.to_excel(path_file_report, index=False)
    create_report_backup(df=df, path_file_report=path_file_report,
                         tag='reencode')

    need_reencode = True
    while need_reencode:
        return_next_video_to_reencode = \
            get_next_video_to_reencode(path_file_report)
        if return_next_video_to_reencode is False:
            logging.info('\nThere are no videos to reencode')
            need_reencode = False
            continue

        dict_video_data = return_next_video_to_reencode
        return_reencode_video = reencode_video(dict_video_data)
        if return_reencode_video is False:
            sys.exit()
            return

        df = update_file_report(path_file_report, dict_video_data)

        # Save reports
        df.to_excel(path_file_report, index=False)
        create_report_backup(df=df, path_file_report=path_file_report,
                             tag='reencode')

    # sys.exit()
    return df


def menu_ask():

    # ptbr
    # print('1-Gerar planilha listando os arquivos')
    # print('2-Processar reencode dos vídeos marcados na coluna '+
    # '"video_resolution_to_change"')
    # print('3-Agrupar vídeos em grupos de até 1 gb com mesmo codec e ' + \
    #   'resolução')

    # eng
    print('1-Generate worksheet listing the files')
    print('2-Process reencode of videos marked in column ' +
          '"video_resolution_to_change"')
    print('3-Group videos into groups up to 1 gb with the same codec ' +
          'and resolution')

    # ptbr
    # msg_type_answer = 'Digite sua resposta: '

    # eng
    msg_type_answer = 'Type your answer: '
    make_report = int(input(f'\n{msg_type_answer}'))
    if make_report == 1:
        return 1
    elif make_report == 2:
        return 2
    elif make_report == 3:
        return 3
    else:
        # ptbr
        # msg_invalid_option = "Opção não disponível"

        # eng
        msg_invalid_option = "Invalid option"
        raise MyValidationError(msg_invalid_option)


def df_insert_row(row_number, df, row_value):
    """
    A customized function to insert a row at any given position in the 
     dataframe.
    source: https://www.geeksforgeeks.org/insert-row-at-given-position-in-pandas-dataframe/
    :input: row_number: Int.
    :input: df: Dataframe.
    :input: row_value: Int.
    :return: Dataframe. df_result | 
             Boolean. False. If the row_number was invalid.
    """

    if row_number > df.index.max()+1:
        print("df_insert_row: Invalid row_number")
        return False

    # Slice the upper half of the dataframe
    df1 = df[0:row_number]

    # Store the result of lower half of the dataframe
    df2 = df[row_number:]

    # Inser the row in the upper half dataframe
    df1.loc[row_number] = row_value

    # Concat the two dataframes
    df_result = pd.concat([df1, df2])

    # Reassign the index labels
    df_result.index = [*range(df_result.shape[0])]

    # Return the updated dataframe
    return df_result


def search_to_split_videos(df, mb_limit):

    def preprocess_df_split(df):

        df['file_path'] = df['file_folder'] + '\\' + df['file_name']
        df['split_file_folder_origin'] = ''
        df['split_file_name_origin'] = ''
        df['split_file_size_origin'] = ''

        return df

    def get_dict_row_dest(dict_row_origin, pathfile_output):

        path_folder_dest = os.path.split(pathfile_output)[0]
        file_name_dest = os.path.split(pathfile_output)[1]
        file_size_dest = os.stat(pathfile_output).st_size

        dict_row_dest = dict_row_origin.copy()
        dict_row_dest['split_file_folder_origin'] = \
            dict_row_origin['file_folder']
        dict_row_dest['split_file_name_origin'] = dict_row_origin['file_name']
        dict_row_dest['split_file_size_origin'] = dict_row_origin['file_size']

        dict_row_dest['file_folder'] = path_folder_dest
        dict_row_dest['file_name'] = file_name_dest
        dict_row_dest['file_size'] = file_size_dest

        return dict_row_dest

    def get_row_number_from_filepath(df, file_path_origin):

        path_folder_origin = os.path.split(file_path_origin)[0]
        file_name_origin = os.path.split(file_path_origin)[1]

        mask1 = df['file_folder'].isin([path_folder_origin])
        mask2 = df['file_name'].isin([file_name_origin])
        mask_file = mask1 & mask2

        df_row_origin = df.loc[mask_file, :]
        row_number = df_row_origin.index.values[0]

        return row_number

    def delete_fileorigin(df, file_path_origin):

        row_number = get_row_number_from_filepath(df, file_path_origin)
        df = df.drop(df.index[[row_number]])
        df = df.reset_index(drop=True)
        return df

    def update_df_files(df, file_path_origin, list_filepath_output):

        def include_rows_new_files(df, filepath_output):

            pathfile_output = os.path.abspath(filepath_output)
            # find the row_number of origin file
            row_number = get_row_number_from_filepath(df, file_path_origin)
            dict_row_origin = df.loc[row_number, :]
            dict_row_dest = get_dict_row_dest(dict_row_origin, pathfile_output)
            df = df_insert_row(row_number=row_number, df=df,
                               row_value=dict_row_dest)
            return df

        for filepath_output in list_filepath_output:
            # include the rows corresponding to the new files created
            df = include_rows_new_files(df=df,
                                        filepath_output=filepath_output)

        # delete the file origin row
        df = delete_fileorigin(df=df, file_path_origin=file_path_origin)

        return df

    def get_list_file_path_origin(df, size_limit):

        mask_to_be_split = df['file_size'] > size_limit
        df_to_be_split = df.loc[mask_to_be_split, :]
        list_file_path_origin = df_to_be_split.loc[:, 'file_path'].tolist()

        return list_file_path_origin

    df = preprocess_df_split(df)

    recoil_sec = 10
    # TODO estimate the recoil_mbsize by video bitrate
    recoil_mbsize = 10
    size_limit = (mb_limit-recoil_mbsize) * 1024**2
    list_file_path_origin = get_list_file_path_origin(df=df,
                                                      size_limit=size_limit)
    folder_script_path = get_folder_script_path()
    output_folder_path = os.path.join(folder_script_path, 'videos_splitted')
    exclude_all_files_from_folder(output_folder_path)

    for file_path_origin in list_file_path_origin:
        list_filepath_output = split_mp4(largefile_path=file_path_origin,
                                         recoil=recoil_sec,
                                         mb_limit=mb_limit,
                                         output_folder_path=output_folder_path)

        df = update_df_files(df=df, file_path_origin=file_path_origin,
                             list_filepath_output=list_filepath_output)

    return df


def userpref_folderoutput():

    path_file = os.path.join('config', 'config.txt')
    variable_name = 'path_folder_output'
    variable_value = \
        handle_config_file(path_file, variable_name,
                           set_value=None, parse=True)
    path_folder_output = variable_value['path_folder_output'][0]

    print(f'Use the folder path output as {path_folder_output}?')
    answer_use = input('(None for yes) Answer: ')
    if answer_use == '':
        pass
    else:
        path_folder_output = input('Inform the folder path output: ')
        handle_config_file(path_file, variable_name,
                           set_value=path_folder_output, parse=False)

    return path_folder_output


def userpref_size_per_file_mb():

    folder_script_path = get_folder_script_path()
    path_file = os.path.join(folder_script_path, 'config', 'config.txt')
    variable_name = 'size_per_file_mb'

    question = 'What should be the maximum size of each ' + \
               'file in mb (e.g.: 500)? '

    variable_value = \
        handle_config_file(path_file, variable_name,
                           set_value=None, parse=True)
    value_got = variable_value[variable_name][0]

    print(f'The maximum size of each file will be {value_got}. Ok?')
    answer_use = input('(None for yes) Answer: ')
    if answer_use == '':
        pass
    else:
        value_got = input(question)
        handle_config_file(path_file, variable_name,
                           set_value=value_got, parse=False)

    return value_got


def ensure_folder_existence(folders_path):
    """
    :input: folders_path: List
    """

    for folder_path in folders_path:
        existence = os.path.isdir(folder_path)
        if existence is False:
            os.mkdir(folder_path)


def ensure_folders_existence():

    folder_script_path_relative = os.path.dirname(__file__)
    folder_script_path = os.path.realpath(folder_script_path_relative)

    folders_name = ['ts', 'videos_encoded',
                    'videos_join', 'config', 'videos_splitted']
    folders_path = []
    for folder_name in folders_name:
        folder_path = os.path.join(folder_script_path, folder_name)
        folders_path.append(folder_path)

    ensure_folder_existence(folders_path)


def get_txt_content(file_path):

    file = open(file_path, 'r', encoding='utf-8')
    file_content = file.readlines()
    file_content = ''.join(file_content)
    file.close()

    return file_content


def create_txt(file_path, stringa):

    f = open(file_path, "w", encoding='utf8')
    f.write(stringa)
    f.close()


def get_folder_name_normalized(path_dir):

    def normalize_string_to_link(string_actual):

        string_new = unidecode.unidecode(string_actual)

        for c in r"!@#$%^&*()[]{};:,./<>?\|`~-=_+":
            string_new = string_new.translate({ord(c): "_"})

        string_new = string_new.replace(' ', '_')
        string_new = string_new.replace('___', '_')
        string_new = string_new.replace('__', '_')

        return string_new

    dir_name = os.path.basename(path_dir)
    dir_name_normalize = normalize_string_to_link(dir_name)

    return dir_name_normalize


def prefill_video_resolution_to_change(df):

    # TODO identify the main main profile that has 'audiocodec aac' and
    #  'videocodec libx264'

    df['key_join_checker'] = df['audio_codec'] + '-' + \
        df['video_codec'] + '-' + \
        df['video_resolution']

    df_key = df[['key_join_checker', 'duration', 'video_resolution']].copy()
    df_key['duration_timedelta'] = pd.to_timedelta(df_key['duration'])
    df_key['duration_min'] = df_key['duration_timedelta'].dt.total_seconds()/60

    df_key.drop('duration', axis=1, inplace=True)

    df_key_agg = \
        df_key.groupby(['key_join_checker',
                        'video_resolution'])['duration_min'].agg('sum')
    # convert in dataframe
    df_key_agg = df_key_agg.reset_index()
    # sort dataframe
    df_key_agg = df_key_agg.sort_values(['duration_min'], ascending=[False])

    print('\n', df_key_agg)
    index_max = df_key_agg['duration_min'].idxmax()
    key_join_main = df_key_agg.loc[index_max, 'key_join_checker']
    video_resolution_main = df_key_agg.loc[index_max, 'video_resolution']

    # informar quantidade de minutos para reencodar
    mask_to_convert = ~df_key_agg.index.isin([index_max])
    minutes_to_reencode = df_key_agg.loc[mask_to_convert, 'duration_min'].sum()
    minutes_total = df_key_agg['duration_min'].sum()
    percent_to_reencode = minutes_to_reencode/minutes_total
    print(f'The main profile is "{key_join_main}"')

    # informar percentual de minutos para reencodar
    # the command ':.1f' fix 1 digit after decimal point
    print(f'There is {minutes_to_reencode:.1f} minutes ' +
          f'({percent_to_reencode*100:.0f}%) to reencode')

    mask_resolution_to_change = ~df['key_join_checker'].isin([key_join_main])
    df.loc[mask_resolution_to_change,
           'video_resolution_to_change'] = video_resolution_main
    df.drop('key_join_checker', axis=1, inplace=True)

    return df


def save_upload_folder_name(path_dir, file_folder_name):

    dir_name_normalize = get_folder_name_normalized(path_dir)
    create_txt(file_path=file_folder_name, stringa=dir_name_normalize)


def create_report_backup(df, path_file_report, tag):

    path_folder = os.path.dirname(path_file_report)
    file_name = os.path.basename(path_file_report)
    file_name_without_extension = os.path.splitext(file_name)[0]
    file_name_backup = file_name_without_extension + "_" + tag + ".xlsx"
    path_file_backup = os.path.join(path_folder, file_name_backup)
    df.to_excel(path_file_backup, index=False)


def step_create_report_filled(path_dir, path_file_report):

    df = gen_report(path_dir)
    # sort path_file by natural human way
    df = df_sort_human(df)
    # prefill column video_resolution_to_change
    df = prefill_video_resolution_to_change(df)
    df.to_excel(path_file_report, index=False)

    # Make backup
    create_report_backup(
        df=df, path_file_report=path_file_report, tag='origin')


def set_make_reencode(path_file_report):

    df = make_reencode(path_file_report)
    df.to_excel(path_file_report, index=False)

    # make backup
    create_report_backup(
        df=df, path_file_report=path_file_report, tag='reencode')
    print('\nReencode finished')


def set_group_column(path_file_report):

    # update video_details with groups

    df = pd.read_excel(path_file_report, engine='openpyxl')
    df = get_video_details_with_group(df)
    df.to_excel(path_file_report, index=False)
    print(f"File '{path_file_report}' was updated with " +
          "group column to fast join\n")
    # backup is not performed here as the grouping can be adjusted manually


def set_split_videos(path_file_report, mb_limit):

    df = pd.read_excel(path_file_report, engine='openpyxl')

    # backup group, after adjusted manually
    create_report_backup(
        df=df, path_file_report=path_file_report, tag='grouped')

    # Find for file_video too big and split them
    df = search_to_split_videos(df, mb_limit=mb_limit)

    df.to_excel(path_file_report, index=False)

    create_report_backup(
        df=df, path_file_report=path_file_report, tag='splited')


def set_join_videos(path_file_report, mb_limit, start_index_output):

    df = pd.read_excel(path_file_report, engine='openpyxl')
    df = join_videos(df, mb_limit, start_index_output)
    df.to_excel(path_file_report, index=False)

    # backup joined
    create_report_backup(
        df=df, path_file_report=path_file_report, tag='joined')


def get_folder_script_path():

    folder_script_path_relative = os.path.dirname(__file__)
    folder_script_path = os.path.realpath(folder_script_path_relative)

    return folder_script_path


def set_path_file_report():

    folder_path_output_relative = 'output_' + get_name_dir_origin()
    ensure_folder_existence([folder_path_output_relative])
    path_file_report = os.path.join(folder_path_output_relative,
                                    'video_details.xlsx')

    return path_file_report


def get_txt_folder_origin():

    file_folder_name = 'folder_files_origin.txt'

    return file_folder_name


def get_start_index_output():

    print('Start output file count with what value?')
    add_num = input('(None for 1) Answer: ')
    if add_num == '':
        add_num = 1
    else:
        add_num = int(add_num)
    return add_num


def main():

    ensure_folders_existence()
    # ensure file exist
    file_name_folder_origin = get_txt_folder_origin()

    try:
        path_file_report = set_path_file_report()
    except:
        pass
    menu_answer = menu_ask()

    if menu_answer == 1:
        # create Dataframe of video details
        path_dir = input('\nPaste the folder link where are the video files: ')

        # save in txt, the folder name
        save_upload_folder_name(path_dir, file_name_folder_origin)

        path_file_report = set_path_file_report()

        step_create_report_filled(path_dir, path_file_report)

        print('\nIf necessary, change the reencode plan in the column ' +
              '"video_resolution_to_change"')

        break_point = input('Type Enter to continue')
        clean_cmd()
        main()
        return

    elif menu_answer == 2:

        # reencode videos mark in column video_resolution_to_change
        set_make_reencode(path_file_report)

        break_point = input('Review the file and then type something to ' +
                            'continue.')
        clean_cmd()
        main()
        return

    elif menu_answer == 3:

        mb_limit = int(userpref_size_per_file_mb())

        # establishes separation criteria for the join videos step
        set_group_column(path_file_report)

        # break_point
        input('Review the file and then type something to ' +
              'start the process that look for videos that ' +
              'are too big and should be splitted')

        set_split_videos(path_file_report, mb_limit)

        # set start index output file
        start_index_output = get_start_index_output()

        # join all videos
        set_join_videos(path_file_report, mb_limit, start_index_output)
        return
    else:

        return


if __name__ == "__main__":
    logging_config()
    main()
