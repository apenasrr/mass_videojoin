from turtle import fd

from utils_mass_videojoin import get_serie_sub_folder

from . import prefill_utils


def include_sub_folder(df):

    serie_file_path_folder = df["file_path_folder"]
    serie_subfolder_n1 = get_serie_sub_folder(serie_file_path_folder)
    df["subfolder_n1"] = serie_subfolder_n1
    return df


def include_video_resolution_to_change(df):

    mask_cv_ok = df['video_codec'].isin(['h264'])
    mask_ca_ok = df['audio_codec'].isin(['aac'])
    mask_isavc = df['is_avc'].isin([1])
    mask_ok = mask_cv_ok & mask_ca_ok & mask_isavc
    df['video_resolution_to_change'] = ''
    df.loc[~mask_ok, 'video_resolution_to_change'] = df.loc[~mask_ok, 'resolution']
    return df


def run(df):
    """
    - Mount ReenCode Plan, filling column 'video_resolution_to_change'.
       Based on:
        - Ensures that video_profile will be codec AVC (is_avc) and audio AAC
    - add columns: 'subfolder_n1', 'resolution'
    - reorder dataframe
    """

    df_update = prefill_utils.include_resolution(df)

    # create column 'video_resolution_to_change'
    df_filled = include_video_resolution_to_change(df_update)

    # create column 'subfolder_n1'
    df_filled = include_sub_folder(df_filled)

    prefill_utils.show_reencode_plan(df_filled)
    return df_filled
