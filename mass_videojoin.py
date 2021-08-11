"""
    Create by: apenasrr
    Source: https://github.com/apenasrr/mass_videojoin

    A smart tool to optimize and make turbo join in a massive video collection
"""

import os
import pandas as pd
import datetime
import logging
import unidecode
import sys
from utils_mass_videojoin import (exclude_all_files_from_folder,
                                  create_report_backup,
                                  get_folder_script_path,
                                  time_is_hh_mm_ss_ms,
                                  sort_human,
                                  sort_df_column_from_list,
                                  check_col_unique_values)
from video_tools import join_mp4, get_duration, \
                        timedelta_to_string, float_seconds_to_string, \
                        float_seconds_from_string
from transition import check_transition_resolution, \
                       get_video_resolution_format, \
                       get_dict_transition_resolution
import make_reencode
from make_split import search_to_split_videos
from configparser import ConfigParser
import video_report
import json


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


def df_sort_human(df, key_column_name):
    """
    Sort files and folders in human way.
    So after folder/file '1' comes '2', instead of '10' and '11'.
    Simple yet flexible natural sorting in Python.
    When you try to sort a list of strings that contain numbers,
    the normal python sort algorithm sorts lexicographically,
    so you might not get the results that you expect:
    More info: https://github.com/SethMMorton/natsort

    :input: DataFrame. With columns [path_file]
    :return: DataFrame. Sort in a human way by [file_path_folder, file_name]
    """

    list_path_file = df[key_column_name].tolist()
    sorter = sort_human(list_path_file)
    df = sort_df_column_from_list(df, key_column_name, sorter)
    return df


def get_serie_sub_folder(serie_folder_path):

    def get_df_sub_folders(serie_folder_path):
        df = serie_folder_path.str.split('\\', expand=True)
        len_cols = len(df.columns)
        list_n_col_to_delete = []
        for n_col in range(len_cols-1):
            serie = df.iloc[:, n_col]
            # check for column with more than 1 unique value (folder root)
            col_has_one_unique_value = check_col_unique_values(serie)
            if col_has_one_unique_value:
                name_col = df.columns[n_col]
                list_n_col_to_delete.append(name_col)

        df = df.drop(list_n_col_to_delete, axis=1)
        return df

    df_sub_folders = get_df_sub_folders(serie_folder_path)
    serie_first_column = df_sub_folders.iloc[:, 0]
    return serie_first_column


def set_mark_group_encode(df):

    df['key_join_checker'] = df['audio_codec'] + '-' + \
        df['video_codec'] + '-' + \
        df['video_resolution_width'].astype(str) + '-' + \
        df['video_resolution_height'].astype(str)
    serie_group_encode_bool = (
        df['key_join_checker'] != df['key_join_checker'].shift(1))
    return serie_group_encode_bool


def set_mask_group_per_folder(serie_folder_path):

    serie_first_column = get_serie_sub_folder(serie_folder_path)
    df_first_column = serie_first_column.to_frame('folder')
    df_first_column['folder_prior'] = df_first_column['folder'].shift(1)
    serie_change_folder_bool = (
        df_first_column['folder_prior'] != df_first_column['folder'])
    return serie_change_folder_bool


def get_serie_group(serie_change_bool):
    """
    from boolean serie, make cumulative sum returning serie int
    true, false, false, true, false
    1, 1, 1, 2, 2
    """

    return serie_change_bool.cumsum()


def get_video_details_with_group(df):

    # set mask group per encode
    serie_group_encode_bool = set_mark_group_encode(df)

    # set mask group per folder
    serie_folder_path = df['file_path_folder_origin']
    serie_change_folder_bool = set_mask_group_per_folder(serie_folder_path)

    # agregate group masks
    serie_change_bool = serie_group_encode_bool | serie_change_folder_bool

    # create group_encode column
    df['group_encode'] = get_serie_group(serie_change_bool)
    return df


def get_list_chunk_videos_from_group(df, group_no, max_size_mb,
                                     duration_limit='00:00:00.00'):

    max_size_bytes = max_size_mb * 1024**2
    mask = df['group_encode'].isin([int(group_no)])

    df['file_path'] = df['file_path_folder'] + '\\' + \
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


def get_path_folder_cache(path_dir):

    dir_name_normalize = get_folder_name_normalized(path_dir)
    folder_name = 'output_' + dir_name_normalize
    ensure_folder_existence([folder_name])

    path_folder_cache = os.path.join(folder_name, 'cache')
    ensure_folder_existence([path_folder_cache])
    return path_folder_cache


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


def join_videos(df, max_size_mb, filename_output,
                path_folder_videos_joined,
                path_folder_videos_cache,
                start_index_output,
                duration_limit='00:00:00.00',
                transition_status=False):
    """join videos according to column 'group_encode' in df dataframe

    Args:
        df (dataframe): video_details dataframe. Required columns:
                         file_dolder, file_name, group_encode
        max_size_mb (int): max size of each block of videos joined
        path_folder_videos_joined (str): destination path_folder
                                          for grouped videos
        path_folder_videos_cache (str): path_folder for cache data
        start_index_output (int): initial number that the exported video files
                                   will receive as a suffix
        duration_limit (str): duration limit in format: hh:mm:ss.ms
        transition_status (bol): true to activate transition effect

    Returns:
        dataframe: video_details dataframe updated with new columns:
                    [file_output, video_origin_duration_pre_join]
    """

    df['file_path'] = df['file_path_folder'] + '\\' + df['file_name']
    list_chunk_videos = get_list_chunk_videos(df, max_size_mb, duration_limit)

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
        file_name_output = f'{filename_output}-%03d.mp4' % file_count
        file_path_output = os.path.join(path_folder_videos_joined,
                                        file_name_output)

        # make video join
        list_dict_videos_duration = join_mp4(list_file_path, file_path_output,
                                             path_folder_videos_cache)

        list_file_path_original = list_chunk_videos_original[index]
        df = join_videos_process_df(df, list_file_path_original,
                                    file_name_output,
                                    list_dict_videos_duration,
                                    transition_status)

        # register file_name_output in dataframe
        mask_files_joined = df['file_path'].isin(list_file_path)
        df.loc[mask_files_joined, 'file_output'] = file_name_output

        df.loc[mask_files_joined, 'file_path_output'] = \
            os.path.abspath(file_path_output)

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


def correct_duration(path_file_report):
    """Corrects the duration metadata in the project report

    Args:
        path_file_report (str): absolute report path file.
            Required columns in report: [file_path_folder, file_name,
                                         duration, duration_seconds]

    Returns:
        dataframe: updated with:
            -corrected duration column
            -new column duration_original
    """

    logging.info('Correcting duration metadata...')

    # set cache folder
    # ensure folder cache in project folder
    project_dir_path = os.path.dirname(path_file_report)
    project_ts_dir_path = os.path.join(project_dir_path, 'cache')
    ensure_folder_existence([project_ts_dir_path])

    # load report project
    df = pd.read_excel(path_file_report, engine='openpyxl')

    # create backup column
    df['duration_original'] = df['duration']
    df['duration_seconds_original'] = df['duration_seconds']

    # iterate through video files
    series_file_path = df['file_path_folder'] + '\\' + df['file_name']
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
        df.loc[index, 'duration_seconds'] = float_duration

        # remove temp file
        exclude_all_files_from_folder(path_folder=project_ts_dir_path)
    return df


def menu_ask():

    print('1-Generate worksheet listing the files')
    print('2-Process reencode of videos marked in column ' +
          '"video_resolution_to_change"')
    print('3-Group videos into groups up to 1 gb with the same codec ' +
          'and resolution')

    msg_type_answer = 'Type your answer: '
    make_report = int(input(f'\n{msg_type_answer}'))
    if make_report == 1:
        return 1
    elif make_report == 2:
        return 2
    elif make_report == 3:
        return 3
    else:
        msg_invalid_option = "Invalid option"
        raise msg_invalid_option


def userpref_size_per_file_mb(size_per_file_mb, path_file_config):

    print(f'The maximum size of each file will be {size_per_file_mb}. Ok?')
    answer_use = input('(None for yes) Answer: ')
    if answer_use == '':
        return size_per_file_mb
    else:
        question_new_value = 'What should be the maximum size of each ' + \
                             'file in mb (e.g.: 500)? '

        new_size_per_file_mb = input(question_new_value)
        config_update_data(path_file_config,
                           'size_per_file_mb',
                           new_size_per_file_mb)
    return new_size_per_file_mb


def get_transition_effect_status(activate_transition):

    if activate_transition == 'true':
        transition_effect_status = True
    else:
        transition_effect_status = False
    return transition_effect_status


def get_duration_limit(duration_limit):

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
    """identify the main profile that has 'audiocodec aac' and
        'videocodec libx264'
    Args:
        df (dataframe): with keys: [audio_codec, video_codec,
                                    video_resolution, duration_seconds, is_avc]

    Returns:
        [dataframe]: Add column 'video_resolution_to_change' filled
    """

    # identify the main profile that has 'audiocodec aac' and
    #  'videocodec libx264'
    # create aux column 'key_join_checker'
    df['key_join_checker'] = df['audio_codec'] + '-' + \
                             df['video_codec'] + '-' + \
                             df['video_resolution_width'].astype(str) + 'x' + \
                             df['video_resolution_height'].astype(str)

    df_key = df[['key_join_checker',
                 'duration_seconds',
                 'is_avc',
                 'video_resolution_width',
                 'video_resolution_height']].copy()

    # add col duration_min
    df_key['duration_min'] = df_key['duration_seconds']/60
    df_key.drop('duration_seconds', axis=1, inplace=True)

    # create a summary dataframe to show sum duration per video profile
    df_key_agg = df_key.groupby(['key_join_checker',
                                 'video_resolution_width',
                                 'video_resolution_height',
                                 'is_avc'])['duration_min'].agg('sum')
    # convert in dataframe
    df_key_agg = df_key_agg.reset_index()
    # sort dataframe
    df_key_agg = df_key_agg.sort_values(['duration_min'], ascending=[False])

    # show table result formated
    df_key_agg_to_show = df_key_agg.copy()
    df_key_agg_to_show['duration_min'] = \
        df_key_agg_to_show['duration_min'].round(1)
    print('\n', df_key_agg_to_show.to_string(index=False))

    # find 'index_max'
    index_max = df_key_agg['duration_min'].idxmax()

    # find 'key_join_main'
    key_join_main = df_key_agg.loc[index_max, 'key_join_checker']

    # show quantity of minutes to reencode
    print(f'The main profile is "{key_join_main}"')

    # find 'video_resolution_main'
    video_resolution_main = \
        df_key_agg.loc[index_max,
                       ['video_resolution_width']][0].astype(str) + 'x' + \
        df_key_agg.loc[index_max,
                       ['video_resolution_height']][0].astype(str)

    # show percentage of minutes to reencode
    mask_to_convert_1 = ~df_key_agg.index.isin([index_max])
    mask_to_convert_2 = ~df_key_agg['is_avc'].isin([1])
    mask_to_convert = mask_to_convert_1 | mask_to_convert_2

    minutes_to_reencode = \
        df_key_agg.loc[mask_to_convert, 'duration_min'].sum()
    minutes_total = df_key_agg['duration_min'].sum()
    percent_to_reencode = minutes_to_reencode/minutes_total
    #  the command ':.1f' fix 1 digit after decimal point
    print(f'There is {minutes_to_reencode:.1f} minutes ' +
          f'({percent_to_reencode*100:.0f}%) to reencode')

    # in main dataframe
    #  add column video_resolution_to_change and fill with main profile
    #   with main profile 'video_resolution_main'
    mask_resolution_to_change_1 = ~df['key_join_checker'].isin([key_join_main])
    mask_resolution_to_change_2 = ~df['is_avc'].isin([1])
    mask_resolution_to_change = mask_resolution_to_change_1 | \
                                mask_resolution_to_change_2
    df['video_resolution_to_change'] = ''
    df.loc[mask_resolution_to_change,
           'video_resolution_to_change'] = video_resolution_main

    # remove aux column 'key_join_checker'
    df.drop('key_join_checker', axis=1, inplace=True)
    return df


def save_metadata_json_files(list_dict_inf_ffprobe, path_file_report):
    """save in project_folder/metadata/ , the metadata of each video file
    in json format

    Args:
        list_dict_inf_ffprobe (list): list of dict of metadata
        path_file_report (str): path_file if videodetails.xlsx
    """

    path_folder_report = os.path.dirname(path_file_report)
    path_folder_metadata = os.path.join(path_folder_report, 'metadata')
    ensure_folder_existence([path_folder_metadata])

    for dict_inf_ffprobe in list_dict_inf_ffprobe:
        path_file_origin = dict_inf_ffprobe['path_file']
        file_name_origin = os.path.basename(path_file_origin)
        file_path_folder_origin = os.path.dirname(path_file_origin)

        file_name_origin_without_ext = os.path.splitext(file_name_origin)[0]
        file_name_json = file_name_origin_without_ext + '.json'

        file_name_dest = \
            make_reencode.get_file_name_dest(file_path_folder_origin,
                                             file_name_json, 'video_metadata_')
        json_path_file = os.path.join(path_folder_metadata, file_name_dest)
        dict_metadata = dict_inf_ffprobe['metadata']
        with open(json_path_file, "w") as fout:
            json.dump(dict_metadata, fout, indent=2)


def step_create_report_filled(path_dir, path_file_report, video_extensions):

    list_file_selected = video_report.get_list_path_video(path_dir, video_extensions)
    list_dict_inf_ffprobe = video_report.get_list_dict_inf_ffprobe(list_file_selected)

    save_metadata_json_files(list_dict_inf_ffprobe, path_file_report)

    list_dict = video_report.gen_report(list_dict_inf_ffprobe)
    df = pd.DataFrame(list_dict)

    # sort path_file by natural human way
    df = df_sort_human(df, key_column_name='path_file')
    # prefill column video_resolution_to_change
    df = prefill_video_resolution_to_change(df)
    df.to_excel(path_file_report, index=False)

    # Make backup
    create_report_backup(
        df=df, path_file_report=path_file_report, tag='origin')


def set_make_reencode(path_file_report, path_folder_videos_encoded):

    df = make_reencode.make_reencode(path_file_report, path_folder_videos_encoded)
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


def set_group_column(path_file_report):

    # update video_details with group_encode column

    df = pd.read_excel(path_file_report, engine='openpyxl')
    df = get_video_details_with_group(df)
    df.to_excel(path_file_report, index=False)
    print(f"File '{path_file_report}' was updated with " +
           "a guide column to fast join (group_encode) \n")

    # Note: backup is not performed here as the
    #       grouping can be adjusted manually


def set_split_videos(path_file_report, mb_limit, path_folder_videos_splitted,
                     duration_limit='00:00:00,00'):

    df = pd.read_excel(path_file_report, engine='openpyxl')

    # backup group, after adjusted manually
    create_report_backup(
        df=df, path_file_report=path_file_report, tag='grouped')

    # Find for file_video too big and split them
    df = search_to_split_videos(df, mb_limit,
                                path_folder_videos_splitted,
                                duration_limit)

    df.to_excel(path_file_report, index=False)

    create_report_backup(
        df=df, path_file_report=path_file_report, tag='splited')


def set_join_videos(path_file_report, mb_limit, filename_output,
                    path_folder_videos_joined,
                    path_folder_videos_cache,
                    duration_limit='00:00:00,00', start_index_output=1,
                    activate_transition='false'):

    df = pd.read_excel(path_file_report, engine='openpyxl')

    transition_status = get_transition_effect_status(activate_transition)
    df = join_videos(df, mb_limit, filename_output,
                     path_folder_videos_joined,
                     path_folder_videos_cache,
                     start_index_output, duration_limit, transition_status)
    df.to_excel(path_file_report, index=False)

    # backup joined
    create_report_backup(
        df=df, path_file_report=path_file_report, tag='joined')


def set_path_file_report(path_dir):

    folder_name_normalized = get_folder_name_normalized(path_dir)
    folder_path_output_relative = 'output_' + folder_name_normalized
    ensure_folder_existence([folder_path_output_relative])
    path_file_report = os.path.join(folder_path_output_relative,
                                    'video_details.xlsx')
    return path_file_report


def set_path_folder_videos_encoded(path_dir):

    dir_name_normalize = get_folder_name_normalized(path_dir)
    folder_path_output_relative = 'output_' + dir_name_normalize
    path_folder_videos_encoded = os.path.join(folder_path_output_relative,
                                              'videos_encoded')
    return path_folder_videos_encoded


def set_path_folder_videos_splitted(path_dir):

    dir_name_normalize = get_folder_name_normalized(path_dir)
    folder_path_output_relative = 'output_' + dir_name_normalize
    path_folder_videos_splitted = os.path.join(folder_path_output_relative,
                                               'videos_splitted')
    return path_folder_videos_splitted


def set_path_folder_videos_joined(path_dir):

    dir_name_normalize = get_folder_name_normalized(path_dir)
    folder_path_output_relative = 'output_' + dir_name_normalize
    path_folder_videos_joined = os.path.join(folder_path_output_relative,
                                             'output_videos')
    return path_folder_videos_joined


def set_path_folder_videos_cache(path_dir):

    dir_name_normalize = get_folder_name_normalized(path_dir)
    folder_path_output_relative = 'output_' + dir_name_normalize
    path_folder_videos_cache = os.path.join(folder_path_output_relative,
                                               'cache')
    return path_folder_videos_cache


def get_config_data(path_file_config):
    """get default configuration data from file config.ini

    Returns:
        dict: config data
    """

    config_file = ConfigParser()
    config_file.read(path_file_config)
    default_config = dict(config_file['default'])
    return default_config


def config_update_data(path_file_config, variable_name, variable_value):

    config = ConfigParser()
    config.read(path_file_config)
    config.set('default', variable_name, variable_value)
    with open(path_file_config, "w+") as config_updated:
        config.write(config_updated)


def get_path_dir(path_dir):

    if path_dir is None:
        path_dir = input('\nPaste the folder link where are the video files: ')
    else:
        pass
    return path_dir


def get_path_file_report(path_file_report, path_dir):

    if path_file_report is None:
        path_file_report = set_path_file_report(path_dir)
    else:
        pass
    return path_file_report


def main():

    folder_script_path = get_folder_script_path()
    path_file_config = os.path.join(folder_script_path, 'config.ini')
    config_data = get_config_data(path_file_config)
    size_per_file_mb = int(config_data['size_per_file_mb'])
    activate_transition = config_data['activate_transition']
    duration_limit = config_data['duration_limit']
    video_extensions = config_data['video_extensions'].split(',')
    start_index = int(config_data['start_index'])
    path_file_report = None
    path_dir = None

    while True:
        menu_answer = menu_ask()
        if menu_answer == 1:
            # create Dataframe of video details

            path_dir = get_path_dir(path_dir)
            path_file_report = set_path_file_report(path_dir)

            step_create_report_filled(path_dir, path_file_report,
                                      video_extensions)

            print('\nIf necessary, change the reencode plan in the column ' +
                  '"video_resolution_to_change"')

            # break_point
            input('Type Enter to continue')
            clean_cmd()
            continue

        elif menu_answer == 2:
            # make reencode
            # correct duration

            path_dir = get_path_dir(path_dir)
            path_file_report = get_path_file_report(path_file_report, path_dir)

            path_folder_videos_encoded = \
                set_path_folder_videos_encoded(path_dir)
            ensure_folder_existence([path_folder_videos_encoded])
            # reencode videos mark in column video_resolution_to_change
            set_make_reencode(path_file_report, path_folder_videos_encoded)

            print('start correcting the duration metadata')

            # correct videos duration
            set_correct_duration(path_file_report)
            print('\nDuration metadata corrected.')

            # break_point
            input('\nType something to go to the main menu, ' +
                  'and proceed to the "Group videos" process.')

            clean_cmd()
            continue

        elif menu_answer == 3:

            # define variables
            path_dir = get_path_dir(path_dir)
            path_file_report = get_path_file_report(path_file_report, path_dir)

            path_folder_videos_splitted = \
                set_path_folder_videos_splitted(path_dir)
            ensure_folder_existence([path_folder_videos_splitted])

            path_folder_videos_joined = \
                set_path_folder_videos_joined(path_dir)
            ensure_folder_existence([path_folder_videos_joined])

            filename_output = get_folder_name_normalized(path_dir)

            path_folder_videos_cache = \
                set_path_folder_videos_cache(path_dir)
            ensure_folder_existence([path_folder_videos_cache])

            mb_limit = int(userpref_size_per_file_mb(size_per_file_mb,
                                                     path_file_config))
            duration_limit = get_duration_limit(duration_limit)

            # establishes separation criteria for the join videos step
            set_group_column(path_file_report)

            # break_point
            input('Review the file and then type something to ' +
                  'start the process that look for videos that ' +
                  'are too big and should be splitted')

            set_split_videos(path_file_report, mb_limit,
                             path_folder_videos_splitted, duration_limit)

            # join all videos
            set_join_videos(path_file_report, mb_limit,
                            filename_output,
                            path_folder_videos_joined,
                            path_folder_videos_cache,
                            duration_limit,
                            start_index,
                            activate_transition)
            return
        else:
            return


if __name__ == "__main__":
    logging_config()
    main()
