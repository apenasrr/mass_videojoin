import pandas as pd
import os
from utils_mass_videojoin import get_folder_script_path, exclude_all_files_from_folder, \
                  time_is_hh_mm_ss_ms, df_insert_row
from video_tools import get_duration, float_seconds_to_string, \
                        float_seconds_from_string, split_mp4


def preprocess_df_split(df):

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
        dict_row_origin['file_path_folder']
    dict_row_dest['split_file_name_origin'] = dict_row_origin['file_name']
    dict_row_dest['split_file_size_origin'] = dict_row_origin['file_size']

    dict_row_dest['file_path_folder'] = path_folder_dest
    dict_row_dest['file_name'] = file_name_dest
    dict_row_dest['file_size'] = file_size_dest

    float_duration = get_duration(pathfile_output)
    str_duration = float_seconds_to_string(float_duration)
    dict_row_dest['duration'] = str_duration
    return dict_row_dest


def get_row_number_from_filepath(df, file_path_origin):

    path_folder_origin = os.path.split(file_path_origin)[0]
    file_name_origin = os.path.split(file_path_origin)[1]

    mask1 = df['file_path_folder'].isin([path_folder_origin])
    mask2 = df['file_name'].isin([file_name_origin])
    mask_file = mask1 & mask2

    df_row_origin = df.loc[mask_file, :]
    row_number = df_row_origin.index.values[0]
    return row_number


def delete_fileorigin(df, file_path_origin):

    df.to_excel('df_delete_fileorigin.xlsx', index=False)
    row_number = get_row_number_from_filepath(df, file_path_origin)

    df = df.drop(df.index[[row_number]])
    df = df.reset_index(drop=True)
    df.to_excel('df_delete_fileorigin_after.xlsx', index=False)
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


def get_mask_to_be_split(df, size_limit, duration_limit='00:00:00.00'):

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
        df (dataframe): columns: file_path, duration, file_size
        size_limit (int): limite size in bytes
        duration_limit (str): duration limit in format: hh:mm:ss.ms

    Returns:
        list: list of dict. keys: [path_file, mb_limit]
    """

    mask_to_be_split = get_mask_to_be_split(df, size_limit,
                                            duration_limit)

    df_to_split = df.loc[mask_to_be_split,
                         ['file_path_folder', 'file_name', 'duration',
                          'file_size']]
    df_to_split['path_file'] = \
        df_to_split['file_path_folder'] + '\\' + df_to_split['file_name']

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
        dict_['path_file'] = row['path_file']
        dict_['mb_limit'] = int(row['mb_limit'])
        str_duration = row['duration']
        dict_['float_duration_sec'] = \
            float_seconds_from_string(str_duration)
        list_dict.append(dict_)
    return list_dict


def search_to_split_videos(df, mb_limit, path_folder_videos_splitted,
                           duration_limit='00:00:00.00'):
    """Searches for videos larger than a certain limit and split them in
       folder 'videos_splitted'.

    Args:
        df (dataframe): video_details.xlsx. Required columns:
                        [file_path_folder, file_name, file_size]
        mb_limit (int): Video size limit  in megabytes
        duration_limit (str, optional): Video duration limit.
                                        Format hh:mm:ss.ms.
                                        Defaults to '00:00:00.00'.
    Returns:
        (dataframe): dataframe updated with new columns:
                     [path_file, split_file_folder_origin,
                      split_file_name_origin, split_file_size_origin]
    """

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

    exclude_all_files_from_folder(path_folder_videos_splitted)
    for dict_path_file_mb_limit in list_dict_path_file_mb_limit:
        file_path = dict_path_file_mb_limit['path_file']
        mb_limit = dict_path_file_mb_limit['mb_limit']
        float_duration_sec = dict_path_file_mb_limit['float_duration_sec']
        list_filepath_output = \
            split_mp4(largefile_path=file_path,
                      recoil=recoil_sec,
                      output_folder_path=path_folder_videos_splitted,
                      mb_limit=mb_limit,
                      original_video_duration_sec=float_duration_sec)
        df = update_df_files(df=df, file_path_origin=file_path,
                             list_filepath_output=list_filepath_output)
    return df
