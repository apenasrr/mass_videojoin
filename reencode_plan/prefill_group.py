from utils_mass_videojoin import get_serie_sub_folder

from . import prefill_utils


def get_df_key(df):

    """
    - create a short dataframe with subfolder_n1,
    - duration in minutes and resolution (height x width)
    - create aux column 'key_join_checker'
    """

    df["key_join_checker"] = (
        df["audio_codec"]
        + "-"
        + df["video_codec"]
        + "-"
        + df["video_resolution_width"].astype(str)
        + "x"
        + df["video_resolution_height"].astype(str)
    )

    df_key = df[
        [
            "key_join_checker",
            "duration_seconds",
            "is_avc",
            "video_resolution_height",
            "video_resolution_width",
            "file_path_folder",
        ]
    ].copy()

    # create column 'subfolder_n1'
    serie_file_path_folder = df_key["file_path_folder"]
    serie_subfolder_n1 = get_serie_sub_folder(serie_file_path_folder)
    df_key["subfolder_n1"] = serie_subfolder_n1

    # add col 'minutes'
    df_key["minutes"] = df_key["duration_seconds"] / 60

    # drop aux columns
    df_key.drop("file_path_folder", axis=1, inplace=True)
    df_key.drop("duration_seconds", axis=1, inplace=True)

    df_key = prefill_utils.include_resolution(df_key)

    return df_key


def get_df_key_subfolder(df_key):

    # create a duration summary dataframe of subfolder_n1
    df_key_subfolder = df_key.groupby(
        [
            "key_join_checker",
            "video_resolution_width",
            "video_resolution_height",
            "is_avc",
            "subfolder_n1",
        ]
    )["minutes"].agg("sum")

    # convert in dataframe
    df_key_subfolder = df_key_subfolder.reset_index()
    # sort dataframe
    df_key_subfolder = df_key_subfolder.sort_values(
        ["subfolder_n1", "minutes"], ascending=[True, False]
    )
    return df_key_subfolder


def get_df_main_profile_per_subfolder(df_key_subfolder):

    df_main_profile_per_subfolder = df_key_subfolder.drop_duplicates(
        subset="subfolder_n1", keep="first"
    ).copy()
    serie_resolution = (
        df_main_profile_per_subfolder["video_resolution_width"].astype(str)
        + "x"
        + df_main_profile_per_subfolder["video_resolution_height"].astype(str)
    )
    df_main_profile_per_subfolder["resolution_main"] = serie_resolution
    df_main_profile_per_subfolder = df_main_profile_per_subfolder.drop(
        ["video_resolution_width", "video_resolution_height"], axis=1
    )
    return df_main_profile_per_subfolder


def get_df_prefill(df_update, df_key, df_main_profile_per_subfolder):

    df = df_update.copy()

    # TODO: Desloc generation of all 'Masks' for separate functions
    # TODO: Add parameter of 'mask list', which should guide the filling of resolution_main

    # add column resolution_main,
    #  by merge df_key with df_main_profile_per_subfolder
    df_merged = df_key.merge(
        df_main_profile_per_subfolder[["subfolder_n1", "resolution_main"]],
        how="left",
        on="subfolder_n1",
    )

    # add column 'subfolder_n1'
    df["subfolder_n1"] = df_merged["subfolder_n1"]

    # mask1: resolution incorrect (not main)
    mask_resolution_to_change_1 = ~(
        df_merged["resolution"].astype(str)
        == df_merged["resolution_main"].astype(str)
    )

    # mask2: is not avc
    mask_resolution_to_change_2 = ~df["is_avc"].isin([1])

    # mask3: audio codec is not aac
    mask_resolution_to_change_3 = ~df["audio_codec"].isin(["aac"])

    # mask mix: to reencode
    mask_resolution_to_change = (
        mask_resolution_to_change_1
        | mask_resolution_to_change_2
        | mask_resolution_to_change_3
    )

    # fill column 'video_resolution_to_change'
    df["video_resolution_to_change"] = ""
    df.loc[
        mask_resolution_to_change, "video_resolution_to_change"
    ] = df_merged.loc[mask_resolution_to_change, "resolution_main"]

    # reorder dataframe
    df_prefill = df.reindex(
        columns=[
            "duration",
            "duration_seconds",
            "total_bitrate",
            "video_codec",
            "video_profile",
            "video_resolution_height",
            "video_resolution_width",
            "video_bitrate",
            "is_avc",
            "audio_codec",
            "file_size",
            "path_file",
            "file_path_folder",
            "resolution",
            "video_resolution_to_change",
            "subfolder_n1",
            "file_name",
        ]
    )
    return df_prefill


def run(df):
    """
    - Mount ReenCode Plan, filling column 'video_resolution_to_change'.
       Based on:
        - Homogen Video_Profile of each 'level 1 subfolder'
        - Maintain higher resolution profile to minimize need to reencode
        - Ensures that the resulting video_profile will be codec AVC (is_avc)
    - add columns: 'subfolder_n1', 'resolution'
    - reorder dataframe
    """
    df_update = prefill_utils.include_resolution(df)
    df_key = get_df_key(df)
    df_key_subfolder = get_df_key_subfolder(df_key)
    df_main_profile_per_subfolder = get_df_main_profile_per_subfolder(
        df_key_subfolder
    )

    df_filled = get_df_prefill(
        df_update, df_key, df_main_profile_per_subfolder
    )
    prefill_utils.show_reencode_plan(df_filled)
    return df_filled

