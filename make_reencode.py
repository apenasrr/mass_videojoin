import pandas as pd
import hashlib
import logging
import os
import sys
from video_tools import change_width_height_mp4, get_video_details
from utils_mass_videojoin import exclude_all_files_from_folder, \
                                 create_report_backup, get_folder_script_path
from ffprobe_micro import ffprobe


def logging_config():

    log_file_name = 'reencode_maker'
    logfilename = 'log-' + log_file_name + '.txt'
    logging.basicConfig(
        level=logging.INFO,
        format=' %(asctime)s-%(levelname)s-%(message)s',
        handlers=[logging.FileHandler(logfilename, 'w', 'utf-8')])
    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter(' %(asctime)s-%(levelname)s-%(message)s')
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)


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


def reencode_video(dict_, path_folder_encoded):

    try:
        video_resolution_to_change = dict_['video_resolution_to_change']
        size_width, size_height = \
            video_resolution_to_change.split('x')
    except:
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

    path_folder_dest = path_folder_encoded
    path_file_dest = os.path.join(path_folder_dest,
                                  file_name_dest)

    # Make video reencode
    logging.info(f'Start reencode: {path_file_origin}')

    change_width_height_mp4(path_file_origin, size_height,
                            size_width, path_file_dest)


def ask_for_delete_old_videos_encode(path_folder_encoded):

    for _, _, files in os.walk(path_folder_encoded):

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


def update_file_report(path_file_report, dict_video_data, path_folder_encoded):

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
    path_folder_dest = path_folder_encoded
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
    # TODO: test column is_avc behavior
    dict_inf_ffprobe = ffprobe(path_file_dest).get_output_as_dict()
    metadata = get_video_details(path_file_dest)
    # register video metadata
    df.loc[index_video, 'bitrate'] = metadata['bitrate']
    df.loc[index_video, 'video_bitrate'] = metadata['video']['bitrate']
    df.loc[index_video, 'video_codec'] = metadata['video']['codec']
    df.loc[index_video, 'is_avc'] = str(dict_inf_ffprobe['streams'][0]['is_avc'])
    df.loc[index_video, 'audio_codec'] = metadata['audio']['codec']
    df.loc[index_video, 'audio_bitrate'] = metadata['audio']['bitrate']
    df.loc[index_video, 'duration'] = metadata['duration']
    df.loc[index_video, 'reencode_done'] = 1
    return df


def make_reencode(path_file_report):

    folder_script_path = get_folder_script_path()
    path_folder_encoded = os.path.join(folder_script_path, 'videos_encoded')

    df = pd.read_excel(path_file_report, engine='openpyxl')
    df = create_backup_columns(df)

    ask_for_delete_old_videos_encode(path_folder_encoded)

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
        return_reencode_video = reencode_video(dict_video_data,
                                               path_folder_encoded)
        if return_reencode_video is False:
            sys.exit()
            return

        # after reencode, update metadata in report, from new videos generated
        df = update_file_report(path_file_report, dict_video_data,
                                path_folder_encoded)

        # Save reports
        df.to_excel(path_file_report, index=False)
        create_report_backup(df=df, path_file_report=path_file_report,
                             tag='reencode')
    return df
