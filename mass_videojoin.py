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
    join_mp4, split_mp4, get_duration, timedelta_to_string, \
    float_seconds_to_string, float_seconds_from_string
from config_handler import handle_config_file
import unidecode
import natsort
import glob
import sys
import hashlib
from transition import check_transition_resolution, \
                       get_video_resolution_format, \
                       get_dict_transition_resolution


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

    # To input more file video extension:
    #  https://dotwhat.net/type/video-movie-files

    tuple_video_extension = (".mp4", ".avi", ".webm", '.ts', '.vob',
                             '.mov', '.mkv', '.wmv')
    str_tuple_video_extension = ', '.join(tuple_video_extension)
    logging.info(f'Find for video with extension: {str_tuple_video_extension}')
    l = []
    for root, _, files in os.walk(path_dir):

        for file in files:
            file_lower = file.lower()
            if file_lower.endswith(tuple_video_extension):
                logging.info(f'Selected file: {file}')

                path_file = os.path.join(root, file)
                dict_inf = get_video_details(path_file)
                # (mode, ino, dev, nlink, uid,
                #  gid, size, atime, mtime, ctime) = os.stat(path_file)
                stats_result = os.stat(path_file)
                mtime = stats_result.st_mtime
                ctime = stats_result.st_ctime

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


def get_list_chunk_videos_from_group(df, group_no, max_size_mb,
                                     duration_limit='00:00:00.00'):

    max_size_bytes = max_size_mb * 1024**2
    mask = df['group_encode'].isin([int(group_no)])

    df['file_path'] = df['file_folder'] + '\\' + \
        df['file_name']

    df_group = df.loc[mask, :]
    df_group['float_duration'] = \
        df_group['duration'].apply(float_seconds_from_string)

    if duration_limit != '00:00:00.00':
        float_duration_limit = float_seconds_from_string(duration_limit)
    else:
        # symbolic limit not attainable
        float_duration_limit = float_seconds_from_string('99:99:99')

    list_chunk_videos = []
    chunk_size = 0
    chunk_duration = 0
    list_videos = []
    for _, row in df_group.iterrows():
        chunk_size_after = chunk_size + row['file_size']
        chunk_duration_after = chunk_duration + row['float_duration']

        if (chunk_size_after > max_size_bytes) or \
           (chunk_duration_after > float_duration_limit):

            logging.info(f'join video from {len(list_videos)} files')
            if len(list_videos) == 0:
                logging.error('There is a video bigger than limit, ' +
                              'after split process.')
                logging.error(row)
                sys.exit()
            list_chunk_videos.append(list_videos)

            list_videos = []
            chunk_size = 0
            chunk_duration = 0

        list_videos.append(row['file_path'])
        chunk_size += row['file_size']
        chunk_duration += row['float_duration']

    if len(list_videos) > 0:
        logging.info(f'join video from {len(list_videos)} files')
        list_chunk_videos.append(list_videos)
        list_videos = []

    logging.info(f'group {group_no} will generate ' +
                 f'{len(list_chunk_videos)} videos')
    return list_chunk_videos


def get_list_chunk_videos(df, max_size_mb, duration_limit='00:00:00.00'):

    list_group = df['group_encode'].unique().tolist()
    list_final = []

    for group_no in list_group:
        group_no = str(group_no)
        list_chunk_videos = \
            get_list_chunk_videos_from_group(df, group_no,
                                             max_size_mb, duration_limit)
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


def join_videos_process_df(df, list_file_path, file_name_output,
                           list_dict_videos_duration,
                           transition_effect=False):
    """"update video_details dataframe with columns:
         file_output, video_duration_real"

    Args:
        df (dataframe): video_details dataframe. Required columns:
                    file_path
        list_file_path (list): list of original video files
        file_name_output (string): file_name of joined video output
        list_dict_videos_duration (list):
            list of dicts, with keys:
                file_path_origin (string). file_path of original video,
                duration_real (string). real video duration, Format hh:mm:ss
        transition_effect {bol}: True if list_file_path contain
                                 transition effects

    Returns:
        dataframe: dataframe updated with columns:
                    file_output, video_duration_real
    """

    # add column file_output
    mask_files_joined = df['file_path'].isin(list_file_path)
    df.loc[mask_files_joined, 'file_output'] = file_name_output

    # add column video_duration_real
    index_video_in_df = 0
    if transition_effect:
        transition_duration_str = list_dict_videos_duration[0]['duration_real']
        # convert to timedelta
        transition_duration = \
            strptimedelta_hh_mm_ss_ms(str_hh_mm_ss_ms=transition_duration_str)
    else:
        transition_duration = \
            strptimedelta_hh_mm_ss_ms(str_hh_mm_ss_ms='00:00:00')

    for dict_videos_duration in list_dict_videos_duration:
        file_path_origin = dict_videos_duration['file_path_origin']

        mask_file = df['file_path'].isin([file_path_origin])
        # if video_path is in dataframe, instead of being a transition video
        if mask_file.any():
            dict_videos_duration = \
                update_dict_videos_duration(dict_videos_duration,
                                            index_video_in_df,
                                            transition_duration)

            index_video_in_df += 1
            string_video_duration_real = dict_videos_duration['duration_real']

            df.loc[mask_file, 'video_duration_real'] = \
                string_video_duration_real
    return df


def join_videos_update_col_duration(df):
    """rename columns durations of video_details dataframe.
        from 'duration' to 'video_origin_duration_pre_join'},
        from 'video_duration_real' to 'duration'

    Args:
        df (dataframe): video_details with columns:
                            duration, video_duration_real

    Returns:
        dataframe: video_details with duration columns renamed
    """

    list_dict_replace = [{'duration': 'video_origin_duration_pre_join'},
                         {'video_duration_real': 'duration'}]
    for dict_ in list_dict_replace:
        df = df.rename(columns=dict_)
    return df


def transition_update_chunk_videos(list_chunk_videos):
    """includes transition effect in the video join plan

    Args:
        list_chunk_videos (list): list of groups.
                                  Each group is a list of video path_files
    """

    def get_transition_path_file(video_path_file):

        video_resolution = get_video_resolution_format(video_path_file)
        dict_transition_resolution = get_dict_transition_resolution()
        transition_path_file = dict_transition_resolution[video_resolution]
        return transition_path_file

    list_chunk_videos_update = []
    for chunk_videos in list_chunk_videos:
        # find transition_path_file based on the resolution of first video_path
        video_path_file = chunk_videos[0]
        transition_path_file = get_transition_path_file(video_path_file)

        for index, video_path in enumerate(chunk_videos):
            if index == 0:
                chunk_videos_update = []
                chunk_videos_update.append(transition_path_file)
            chunk_videos_update.append(video_path)
            chunk_videos_update.append(transition_path_file)
        list_chunk_videos_update.append(chunk_videos_update)
    return list_chunk_videos_update


def time_is_hh_mm_ss_ms(str_hh_mm_ss_ms):
    """test if time value is format hh:mm:ss.ms

    Args:
        str_hh_mm_ss_ms (str): time value

    Raises:
        Exception: incorrrect format

    Returns:
        bol: True if valid
    """

    try:
        hr, min, sec = map(float, str_hh_mm_ss_ms.split(':'))
        return True
    except:
        raise Exception(f'The time value "{str_hh_mm_ss_ms} "' +
                        'need to be in format: hh:mm:ss.ms')


def strptimedelta_hh_mm_ss_ms(str_hh_mm_ss_ms):

    hr, min, sec = map(float, str_hh_mm_ss_ms.split(':'))
    duration_timedelta = datetime.timedelta(hours=hr, minutes=min, seconds=sec)
    return duration_timedelta


def ensure_transitions(list_chunk_videos):
    """ensures that there is an appropriate transition, based on resolution,
        for each chunk_videos

    Args:
        list_chunk_videos (list): list of chunk_videos.
                                  Chunk_videos are list of video path_file
    """

    list_path_file_chunk_representatives = []
    for chunk_videos in list_chunk_videos:
        first_path_file = chunk_videos[0]
        list_path_file_chunk_representatives.append(first_path_file)

    check_transition_resolution(list_path_file_chunk_representatives)


def join_videos(df, max_size_mb, start_index_output,
                duration_limit='00:00:00.00', transition_status=False):
    """join videos according to column 'group_encode' in df dataframe

    Args:
        df (dataframe): video_details dataframe. Required columns:
                         file_dolder, file_name, group_encode
        max_size_mb (int): max size of each block of videos joined
        start_index_output (int): initial number that the exported video files
                                   will receive as a suffix
        duration_limit (str): duration limit in format: hh:mm:ss.ms
        transition_status (bol): true to activate transition effect

    Returns:
        dataframe: video_details dataframe updated with new columns:
                    [file_output, video_origin_duration_pre_join]
    """

    path_folder_output = get_path_folder_output_video()
    default_filename_output = get_name_dir_origin()

    df['file_path'] = df['file_folder'] + '\\' + df['file_name']
    list_chunk_videos = get_list_chunk_videos(df, max_size_mb, duration_limit)

    # break point
    input('Press a key to continue...')

    df['file_output'] = ''

    list_chunk_videos_original = list_chunk_videos.copy()
    # make list_chunk_videos with transition effect
    if transition_status:
        ensure_transitions(list_chunk_videos)
        # include transition_video between each file in list_chunk_videos
        list_chunk_videos = \
            transition_update_chunk_videos(list_chunk_videos)

    for index, list_file_path in enumerate(list_chunk_videos):
        file_count = index + start_index_output
        file_name_output = f'{default_filename_output}-%03d.mp4' % file_count
        file_path_output = os.path.join(path_folder_output, file_name_output)

        # make video join
        list_dict_videos_duration = join_mp4(list_file_path, file_path_output)

        list_file_path_original = list_chunk_videos_original[index]
        df = join_videos_process_df(df, list_file_path_original,
                                    file_name_output,
                                    list_dict_videos_duration,
                                    transition_status)

        # register file_name_output in dataframe
        mask_files_joined = df['file_path'].isin(list_file_path)
        df.loc[mask_files_joined, 'file_output'] = file_name_output

    df = join_videos_update_col_duration(df)
    print(f'total: {len(list_chunk_videos)} videos')
    return df


def update_dict_videos_duration(dict_videos_duration, index,
                                transition_duration):
    """update video_duration key in dict, with duration if transition effects

    Args:
        dict_videos_duration (dict): required key 'duration_real'
        index (int): index position in group videos
        transition_duration (timedelta): video transition duration

    Returns:
        dict: dict_videos_duration updated
    """

    if index == 0:
        plus_timedelta = transition_duration + transition_duration
    else:
        plus_timedelta = transition_duration

    duration_pre_transition = \
        dict_videos_duration['duration_real']

    duration_pre_transition_timedelta = \
        strptimedelta_hh_mm_ss_ms(
            str_hh_mm_ss_ms=duration_pre_transition)

    duration_pos_transition_timedelta = \
        duration_pre_transition_timedelta + \
        plus_timedelta

    duration_pos_transition_str = \
        timedelta_to_string(duration_pos_transition_timedelta)
    dict_videos_duration['duration_real'] = duration_pos_transition_str
    return dict_videos_duration


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

        path_folder_dest = os.path.join(folder_script_path, 'videos_encoded')
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
    return df


def correct_duration(path_file_report):
    """Corrects the duration metadata in the project report

    Args:
        path_file_report (str): absolute report path file.
            Required columns in report: ['file_folder', 'file_name']

    Returns:
        dataframe: updated with:
            -corrected duration column
            -new column duration_original
    """

    logging.info('Correcting duration metadata...')

    # ensure folder ts in project folder
    project_dir_path = os.path.dirname(path_file_report)
    project_ts_dir_path = os.path.join(project_dir_path, 'ts')
    ensure_folder_existence([project_ts_dir_path])

    # load report project
    df = pd.read_excel(path_file_report, engine='openpyxl')

    # create backup column
    df['duration_original'] = df['duration']

    # iterate through video files
    series_file_path = df['file_folder'] + '\\' + df['file_name']
    list_file_path = series_file_path.tolist()
    for index, file_path in enumerate(list_file_path):

        # convert file
        file_name_ts = f'{index+1}.ts'
        path_file_name_ts = os.path.join(
            project_ts_dir_path, file_name_ts)
        os.system("ffmpeg -i " + '"' + file_path + '"' +
                  " -c copy -bsf:v h264_mp4toannexb -f mpegts " +
                  path_file_name_ts)

        # get duration
        float_duration = get_duration(path_file_name_ts)
        string_duration = \
            float_seconds_to_string(float_duration)

        # include in report file
        df.loc[index, 'duration'] = string_duration

        # remove temp file
        exclude_all_files_from_folder(path_folder=project_ts_dir_path)
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
        raise msg_invalid_option


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


def search_to_split_videos(df, mb_limit, duration_limit='00:00:00.00'):
    """Searches for videos larger than a certain limit and split them in
       folder 'videos_splitted'.

    Args:
        df (dataframe): video_details.xlsx. Required columns:
                        [file_folder, file_name, file_size]
        mb_limit (int): Video size limit  in megabytes
        duration_limit (str, optional): Video duration limit.
                                        Format hh:mm:ss.ms.
                                        Defaults to '00:00:00.00'.
    Returns:
        (dataframe): dataframe updated with new columns:
                     [file_path, split_file_folder_origin,
                      split_file_name_origin, split_file_size_origin]
    """

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

        float_duration = get_duration(pathfile_output)
        str_duration = float_seconds_to_string(float_duration)
        dict_row_dest['duration'] = str_duration
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

    def get_mask_duration_longer_than(serie_str_duration, str_duration_limit):
        """

        Args:
            serie_str_duration (series): string duration series in format:
                                        hh:mm:ss.ms
            str_duration_limit ([type]): string duration in format: hh:mm:ss.ms

        Returns:
            series: boolean mask series. True = longer than limit.
        """

        serie_timedelta_duration = pd.to_timedelta(serie_str_duration)
        timedelta_duration_limit = pd.to_timedelta(str_duration_limit)
        mask_duration_limit = \
            serie_timedelta_duration > timedelta_duration_limit
        return mask_duration_limit

    def get_mask_to_be_split(df, size_limit,
                             duration_limit='00:00:00.00'):

        """[summary]

        Returns:
            (series bol): mask to df indicate which rows
                            need to be split

        """

        mask_size = df['file_size'] > size_limit

        if duration_limit != '00:00:00.00':
            serie_str_duration = df['duration']
            mask_duration = \
                get_mask_duration_longer_than(
                    serie_str_duration=serie_str_duration,
                    str_duration_limit=duration_limit)
            mask_to_be_split = mask_size | mask_duration
        else:
            mask_to_be_split = mask_size
        return mask_to_be_split

    def get_list_dict_path_file_mb_limit(df, size_limit,
                                         duration_limit='00:00:00.00'):
        """get list of the files to be splited and
        their maximum split size in megabyte

        Args:
            df (dataframe): [description]
            size_limit (int): limite size in bytes
            duration_limit (str): duration limit in format: hh:mm:ss.ms

        Returns:
            list: list of dict. keys: [path_file, mb_limit]
        """

        mask_to_be_split = get_mask_to_be_split(df, size_limit,
                                                duration_limit)

        df_to_split = df.loc[mask_to_be_split,
                            ['file_folder', 'file_name', 'duration',
                             'file_size']]
        df_to_split['file_path'] = df_to_split['file_folder'] + '\\' + \
                                   df_to_split['file_name']

        if duration_limit != '00:00:00.00':
            # col proportion_duration_limit: divide video duration
            #                                by the duration limit.
            df_to_split['timedelta_duration'] = \
                pd.to_timedelta(df_to_split['duration'])
            timedelta_duration_limit = pd.to_timedelta(duration_limit)
            df_to_split['proportion_duration_limit'] = \
                df_to_split['timedelta_duration'] / timedelta_duration_limit

            # col size_limit_by_duration
            df_to_split['size_limit_by_duration'] = \
                df_to_split['file_size'] // \
                    df_to_split['proportion_duration_limit']

            # col size_split: lowest value between
            #                 size_limit_by_duration and size_limit
            df_to_split['size_limit'] = size_limit
            df_to_split['size_split'] = df_to_split[['size_limit_by_duration',
                                                    'size_limit']].min(axis=1)

            # col mb_limit: convert size_split from bytes to mb
            df_to_split['mb_limit'] = df_to_split['size_split'] // (1024 ** 2)
        else:
            df_to_split['mb_limit'] = df_to_split['size_limit'] // (1024 ** 2)

        list_dict = []
        for _, row in df_to_split.iterrows():
            dict_ = {}
            dict_['file_path'] = row['file_path']
            dict_['mb_limit'] = int(row['mb_limit'])
            str_duration = row['duration']
            dict_['float_duration_sec'] = \
                float_seconds_from_string(str_duration)
            list_dict.append(dict_)
        return list_dict

    df = preprocess_df_split(df)

    recoil_sec = 10

    # TODO: estimate the recoil_mbsize by video bitrate. Set 10 mb arbitrarily
    recoil_mbsize = 10
    size_limit = (mb_limit - recoil_mbsize) * 1024 ** 2

    if duration_limit != '00:00:00.00':
        # ensure duration_limit is valid or raise error
        time_is_hh_mm_ss_ms(str_hh_mm_ss_ms=duration_limit)

    list_dict_path_file_mb_limit = \
        get_list_dict_path_file_mb_limit(df=df,
                                         size_limit=size_limit,
                                         duration_limit=duration_limit)

    folder_script_path = get_folder_script_path()
    output_folder_path = os.path.join(folder_script_path, 'videos_splitted')
    exclude_all_files_from_folder(output_folder_path)

    for dict_path_file_mb_limit in list_dict_path_file_mb_limit:
        file_path = dict_path_file_mb_limit['file_path']
        mb_limit = dict_path_file_mb_limit['mb_limit']
        float_duration_sec = dict_path_file_mb_limit['float_duration_sec']
        list_filepath_output = \
            split_mp4(largefile_path=file_path,
                      recoil=recoil_sec,
                      mb_limit=mb_limit,
                      output_folder_path=output_folder_path,
                      original_video_duration_sec=float_duration_sec)

        df = update_df_files(df=df, file_path_origin=file_path,
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


def get_transition_effect_status():

    folder_script_path = get_folder_script_path()
    path_file = os.path.join(folder_script_path, 'config', 'config.txt')
    variable_name = 'activate_transition'
    variable_value = \
        handle_config_file(path_file, variable_name,
                           set_value=None, parse=True)
    transition_effect_status = variable_value['activate_transition'][0]
    if transition_effect_status == 'true':
        transition_effect_status = True
    else:
        transition_effect_status = False
    return transition_effect_status


def get_duration_limit():

    folder_script_path = get_folder_script_path()
    path_file = os.path.join(folder_script_path, 'config', 'config.txt')
    variable_name = 'duration_limit'
    variable_value = \
        handle_config_file(path_file, variable_name,
                           set_value=None, parse=True)
    duration_limit = variable_value[variable_name][0]
    # ensure duration_limit is valid or raise error
    time_is_hh_mm_ss_ms(str_hh_mm_ss_ms=duration_limit)
    return duration_limit


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

    # identify the main main profile that has 'audiocodec aac' and
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


def set_correct_duration(path_file_report):

    df = correct_duration(path_file_report)

    df.to_excel(path_file_report, index=False)

    # make backup
    create_report_backup(
        df=df, path_file_report=path_file_report, tag='correct_duration')
    print('\nCorrected Duration column')


def set_group_column(path_file_report):

    # update video_details with groups

    df = pd.read_excel(path_file_report, engine='openpyxl')
    df = get_video_details_with_group(df)
    df.to_excel(path_file_report, index=False)
    print(f"File '{path_file_report}' was updated with " +
          "group column to fast join\n")

    # Note: backup is not performed here as the
    #       grouping can be adjusted manually


def set_split_videos(path_file_report, mb_limit, duration_limit='00:00:00,00'):

    df = pd.read_excel(path_file_report, engine='openpyxl')

    # backup group, after adjusted manually
    create_report_backup(
        df=df, path_file_report=path_file_report, tag='grouped')

    # Find for file_video too big and split them
    df = search_to_split_videos(df, mb_limit=mb_limit,
                                duration_limit=duration_limit)

    df.to_excel(path_file_report, index=False)

    create_report_backup(
        df=df, path_file_report=path_file_report, tag='splited')


def set_join_videos(path_file_report, mb_limit, duration_limit='00:00:00,00',
                    start_index_output=1):

    df = pd.read_excel(path_file_report, engine='openpyxl')

    # set in config/config.txt if transition_effect are true or false
    transition_status = get_transition_effect_status()
    df = join_videos(df, mb_limit, start_index_output,
                     duration_limit, transition_status)
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

        # break_point
        input('Type Enter to continue')
        clean_cmd()
        main()
        return

    elif menu_answer == 2:

        # reencode videos mark in column video_resolution_to_change
        set_make_reencode(path_file_report)

        # break_point
        input('type something to start correcting the duration metadata...')
        set_correct_duration(path_file_report)

        # break_point
        input('Review the file and then type something ' +
              'to go to the main menu.')

        clean_cmd()
        main()
        return

    elif menu_answer == 3:

        # set start index output file
        start_index_output = get_start_index_output()

        mb_limit = int(userpref_size_per_file_mb())
        duration_limit = get_duration_limit()

        # establishes separation criteria for the join videos step
        set_group_column(path_file_report)

        # break_point
        input('Review the file and then type something to ' +
              'start the process that look for videos that ' +
              'are too big and should be splitted')



        set_split_videos(path_file_report, mb_limit, duration_limit)

        # join all videos
        set_join_videos(path_file_report, mb_limit, duration_limit,
                        start_index_output)
        return
    else:
        return


if __name__ == "__main__":
    logging_config()
    main()
