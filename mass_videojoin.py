"""
    Create by: apenasrr
    Source: https://github.com/apenasrr/mass_videojoin

    A smart tool to optimize and make turbo join in a massive video collection
"""

import datetime
import json
import logging
import os
import shutil
import sys
from configparser import ConfigParser

import pandas as pd
import unidecode

import make_reencode
import video_report
from make_split import search_to_split_videos
from reencode_plan import prefill
from transition import (check_transition_resolution,
                        get_dict_transition_resolution,
                        get_video_resolution_format)
from utils_mass_videojoin import (create_report_backup,
                                  exclude_all_files_from_folder,
                                  get_folder_script_path, get_serie_sub_folder,
                                  normalize_string, sort_df_column_from_list,
                                  sort_human, time_is_hh_mm_ss_ms)
from video_tools import (float_seconds_from_string, float_seconds_to_string,
                         get_dict_video_duration, get_duration, join_mp4,
                         timedelta_to_string)


def logging_config():

    logfilename = "log-" + "mass_videojoin" + ".txt"
    logging.basicConfig(
        filename=logfilename,
        level=logging.INFO,
        format=" %(asctime)s-%(levelname)s-%(message)s",
    )
    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter(" %(asctime)s-%(levelname)s-%(message)s")
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger("").addHandler(console)


def clean_cmd():
    def clear():
        return os.system("cls")

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

    key_column_name_norm = key_column_name + "_norm"
    df[key_column_name_norm] = df[key_column_name].apply(normalize_string)
    list_path_file = df[key_column_name_norm].tolist()
    sorter = sort_human(list_path_file)
    df = sort_df_column_from_list(df, key_column_name_norm, sorter)
    df = df.drop([key_column_name_norm], axis=1)
    df = df.reset_index()
    return df


def set_mark_group_encode(df):

    df["key_join_checker"] = (
        df["audio_codec"]
        + "-"
        + df["video_codec"]
        + "-"
        + df["video_resolution_width"].astype(str)
        + "-"
        + df["video_resolution_height"].astype(str)
    )
    serie_group_encode_bool = df["key_join_checker"] != df["key_join_checker"].shift(1)
    return serie_group_encode_bool


def set_mask_group_per_folder(serie_folder_path):

    serie_first_column = get_serie_sub_folder(serie_folder_path)
    df_first_column = serie_first_column.to_frame("folder")
    df_first_column["folder_prior"] = df_first_column["folder"].shift(1)
    serie_change_folder_bool = (
        df_first_column["folder_prior"] != df_first_column["folder"]
    )
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
    serie_folder_path = df["file_path_folder_origin"]
    serie_change_folder_bool = set_mask_group_per_folder(serie_folder_path)

    # agregate group masks
    serie_change_bool = serie_group_encode_bool | serie_change_folder_bool

    # create group_encode column
    df["group_encode"] = get_serie_group(serie_change_bool)
    return df


def get_list_chunk_videos_from_group(
    df, group_no, max_size_mb, duration_limit="00:00:00.00"
):

    max_size_bytes = max_size_mb * 1024 ** 2
    mask = df["group_encode"].isin([int(group_no)])

    df["file_path"] = df["file_path_folder"] + "\\" + df["file_name"]

    df_group = df.loc[mask, :]
    df_group["float_duration"] = df_group["duration"].apply(float_seconds_from_string)

    if duration_limit != "00:00:00.00":
        float_duration_limit = float_seconds_from_string(duration_limit)
    else:
        # symbolic limit not attainable
        float_duration_limit = float_seconds_from_string("99:99:99")

    list_chunk_videos = []
    chunk_size = 0
    chunk_duration = 0
    list_videos = []
    for _, row in df_group.iterrows():
        chunk_size_after = chunk_size + row["file_size"]
        chunk_duration_after = chunk_duration + row["float_duration"]

        if (chunk_size_after > max_size_bytes) or (
            chunk_duration_after > float_duration_limit
        ):

            logging.info(f"join video from {len(list_videos)} files")
            if len(list_videos) == 0:
                logging.error(
                    "There is a video bigger than limit, " + "after split process."
                )
                logging.error(row)
                sys.exit()
            list_chunk_videos.append(list_videos)

            list_videos = []
            chunk_size = 0
            chunk_duration = 0

        list_videos.append(row["file_path"])
        chunk_size += row["file_size"]
        chunk_duration += row["float_duration"]

    if len(list_videos) > 0:
        logging.info(f"join video from {len(list_videos)} files")
        list_chunk_videos.append(list_videos)
        list_videos = []

    logging.info(
        f"group {group_no} will generate " + f"{len(list_chunk_videos)} videos"
    )
    return list_chunk_videos


def get_list_chunk_videos(df, max_size_mb, duration_limit="00:00:00.00"):

    list_group = df["group_encode"].unique().tolist()
    list_final = []

    for group_no in list_group:
        group_no = str(group_no)
        list_chunk_videos = get_list_chunk_videos_from_group(
            df, group_no, max_size_mb, duration_limit
        )
        list_final += list_chunk_videos
        print("")
    return list_final


def get_path_folder_cache(path_dir):

    dir_name_normalize = get_folder_name_normalized(path_dir)
    folder_name = os.path.join("projects", "output_" + dir_name_normalize)
    ensure_folder_existence([folder_name])

    path_folder_cache = os.path.join(folder_name, "cache")
    ensure_folder_existence([path_folder_cache])
    return path_folder_cache


def join_videos_process_df(
    df,
    list_dict_videos_duration,
    transition_effect=False,
):
    """"update video_details dataframe with columns:
         file_output, video_duration_real"

    Args:
        df (dataframe): video_details dataframe. Required columns:
                    file_path
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

    # add column video_duration_real
    index_video_in_df = 0
    if transition_effect:
        transition_duration_str = list_dict_videos_duration[0]["duration_real"]
        # convert to timedelta
        transition_duration = strptimedelta_hh_mm_ss_ms(
            str_hh_mm_ss_ms=transition_duration_str
        )
    else:
        transition_duration = strptimedelta_hh_mm_ss_ms(str_hh_mm_ss_ms="00:00:00")

    for dict_videos_duration in list_dict_videos_duration:
        file_path_origin = dict_videos_duration["file_path_origin"]

        mask_file = df["file_path"].isin([file_path_origin])
        # if video_path is in dataframe, instead of being a transition video
        if mask_file.any():
            dict_videos_duration = update_dict_videos_duration(
                dict_videos_duration, index_video_in_df, transition_duration
            )

            index_video_in_df += 1
            string_video_duration_real = dict_videos_duration["duration_real"]

            df.loc[mask_file, "video_duration_real"] = string_video_duration_real
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

    list_dict_replace = [
        {"duration": "video_origin_duration_pre_join"},
        {"video_duration_real": "duration"},
    ]
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

    hr, min, sec = map(float, str_hh_mm_ss_ms.split(":"))
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


def do_videos_join(
    list_file_path, file_path_output, path_folder_videos_cache,
):
    """Process videos join from a list of video path

    Args:
        list_file_path (list): list of video path

    Returns:
        list: dict with keys: file_path_origin, duration_real
    """

    # remove file_path_output if already exists
    if os.path.exists(file_path_output):
        os.remove(file_path_output)

    if len(list_file_path) == 1:
        # Block with 1 video is not necessary to join
        single_video_file_path = list_file_path[0]
        dict_videos_duration = get_dict_video_duration(
            single_video_file_path, single_video_file_path
        )
        list_dict_videos_duration = [dict_videos_duration]

        shutil.copyfile(list_file_path[0], file_path_output)

    else:
        # make video join
        list_dict_videos_duration = join_mp4(
            list_file_path, file_path_output, path_folder_videos_cache
        )
    return list_dict_videos_duration


def join_videos(
    file_path_report,
    max_size_mb,
    filename_output,
    path_folder_videos_joined,
    path_folder_videos_cache,
    start_index_output,
    duration_limit="00:00:00.00",
    transition_status=False,
):
    """join videos according to column 'group_encode' in df dataframe

    Args:
        file_path_report (string): file path of video_details report.
                                   Required columns:
                                       [file_dolder, file_name, group_encode]
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

    def create_column_number_block_file_output(df,
                                                list_chunk_videos,
                                                start_index_output):

        for index, chunk_videos in enumerate(list_chunk_videos):

            mask = df["file_path"].isin(chunk_videos)

            # add column number_block
            df.loc[mask, "number_block"] = index + start_index_output

            # add column file_output
            file_count = index + start_index_output
            file_name_output = f"{filename_output}-%03d.mp4" % file_count
            file_path_output = os.path.join(path_folder_videos_joined,
                                            file_name_output)
            df.loc[mask, "file_output"] = file_path_output

            # df.loc["file_path_output"] = os.path.abspath(file_path_output)

        return df

    def get_next_join_job(file_path_report):

        # load dataframe
        try:
            df = pd.read_excel(file_path_report, engine="openpyxl")
        except Exception as e:
            logging.error(f"Can't open file: {file_path_report}")
            logging.error(e)

        # create mask to join
        mask_df_to_join = df["join_done"].isin([0])

        # filter df to join
        df_to_join = df.loc[mask_df_to_join, :].reset_index(drop=True)
        if df_to_join.shape[0] == 0:
            return False

        # check if there is videos to join. Return False if note
        file_output = df_to_join.loc[0, "file_output"]

        mask_job = df_to_join['file_output'].isin([file_output])
        list_file_path = df_to_join.loc[mask_job, 'file_path'].to_list()

        list_join_job = [file_output, list_file_path]

        return list_join_job

    def mark_join_job_done(df, file_output):

        mask = df['file_output'].isin([file_output])
        df.loc[mask, 'join_done'] = 1
        return df

    df = pd.read_excel(file_path_report, engine="openpyxl")

    # if it's the first time running the join process
    if "join_done" not in df.columns:
        # Create columns: join_done, file_path, number_block, file_output
        df['join_done'] = 0
        df["file_path"] = df["file_path_folder"] + "\\" + df["file_name"]
        list_chunk_videos = get_list_chunk_videos(df,
                                                  max_size_mb,
                                                  duration_limit)

        df = create_column_number_block_file_output(df,
                                                    list_chunk_videos,
                                                    start_index_output)

        create_report_backup(df=df,
                             path_file_report=file_path_report,
                             tag="6_join_plan")
        df.to_excel(file_path_report, index=False)

    # process each video block
    need_join = True
    while need_join:
        list_join_job = get_next_join_job(file_path_report)
        if list_join_job is False:
            logging.info("\nThere are no more videos to join")
            need_join = False
            continue

        file_output, list_file_path = list_join_job

        # add transition effect if applicable
        list_file_path_to_join = list_file_path.copy()
        if transition_status:
            list_file_path_to_join = transition_update_chunk_videos(list_file_path)

        while True:
            # Do videos join
            list_dict_videos_duration = do_videos_join(
                list_file_path_to_join, file_output, path_folder_videos_cache,
            )

            # check consistency of file_output
            size_origin = sum([os.path.getsize(x) for x in list_file_path_to_join])
            size_final = os.path.getsize(file_output)
            if size_final > size_origin*0.9:
                break

        # Update report
        df = join_videos_process_df(
            df,
            list_dict_videos_duration,
            transition_status,
        )

        df = mark_join_job_done(df, file_output)


        create_report_backup(df=df,
                             path_file_report=file_path_report,
                             tag="7_joined")
        df.to_excel(file_path_report, index=False)

    # update col duration name after adjust by join
    df = join_videos_update_col_duration(df)

    return df


def update_dict_videos_duration(dict_videos_duration, index, transition_duration):
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

    duration_pre_transition = dict_videos_duration["duration_real"]

    duration_pre_transition_timedelta = strptimedelta_hh_mm_ss_ms(
        str_hh_mm_ss_ms=duration_pre_transition
    )

    duration_pos_transition_timedelta = (
        duration_pre_transition_timedelta + plus_timedelta
    )

    duration_pos_transition_str = timedelta_to_string(duration_pos_transition_timedelta)
    dict_videos_duration["duration_real"] = duration_pos_transition_str
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

    logging.info("Correcting duration metadata...")

    # set cache folder
    # ensure folder cache in project folder
    project_dir_path = os.path.dirname(path_file_report)
    project_ts_dir_path = os.path.join(project_dir_path, "cache")
    ensure_folder_existence([project_ts_dir_path])

    # load report project
    df = pd.read_excel(path_file_report, engine="openpyxl")

    # create backup column
    df["duration_original"] = df["duration"]
    df["duration_seconds_original"] = df["duration_seconds"]

    # iterate through video files
    series_file_path = df["file_path_folder"] + "\\" + df["file_name"]
    list_file_path = series_file_path.tolist()
    for index, file_path in enumerate(list_file_path):

        # convert file
        file_name_ts = f"{index+1}.ts"
        path_file_name_ts = os.path.join(project_ts_dir_path, file_name_ts)
        os.system(
            "ffmpeg -i "
            + '"'
            + file_path
            + '"'
            + " -c copy -bsf:v h264_mp4toannexb -f mpegts "
            + path_file_name_ts
        )

        # get duration
        float_duration = get_duration(path_file_name_ts)
        string_duration = float_seconds_to_string(float_duration)

        # include in report file
        df.loc[index, "duration"] = string_duration
        df.loc[index, "duration_seconds"] = float_duration

        # remove temp file
        exclude_all_files_from_folder(path_folder=project_ts_dir_path)
    return df


def menu_ask():

    # fmt: off
    print("1-Generate worksheet listing the files")
    print("2-Process reencode of videos marked in column "
          '"video_resolution_to_change"')
    print("3-Group videos into groups up to 1 gb with the same codec "
          "and resolution")

    msg_type_answer = "Type your answer: "
    make_report = int(input(f"\n{msg_type_answer}"))
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

    print(f"The maximum size of each file will be {size_per_file_mb}. Ok?")
    answer_use = input("(None for yes) Answer: ")
    if answer_use == "":
        return size_per_file_mb
    else:
        # fmt: off
        question_new_value = ("What should be the maximum size of each "
                              "file in mb (e.g.: 500)? ")

        new_size_per_file_mb = input(question_new_value)
        # fmt: on
        config_update_data(path_file_config,
                           "size_per_file_mb",
                           new_size_per_file_mb)
    return new_size_per_file_mb


def get_transition_effect_status(activate_transition):

    if activate_transition == "true":
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

        string_new = string_new.replace(" ", "_")
        string_new = string_new.replace("___", "_")
        string_new = string_new.replace("__", "_")
        return string_new

    dir_name = os.path.basename(path_dir)
    dir_name_normalize = normalize_string_to_link(dir_name)
    return dir_name_normalize


def save_metadata_json_files(list_dict_inf_ffprobe, path_file_report):
    """save in project_folder/metadata/ , the metadata of each video file
    in json format

    Args:
        list_dict_inf_ffprobe (list): list of dict of metadata
        path_file_report (str): path_file if videodetails.xlsx
    """

    path_folder_report = os.path.dirname(path_file_report)
    path_folder_metadata = os.path.join(path_folder_report, "metadata")
    ensure_folder_existence([path_folder_metadata])

    for dict_inf_ffprobe in list_dict_inf_ffprobe:
        path_file_origin = dict_inf_ffprobe["path_file"]
        file_name_origin = os.path.basename(path_file_origin)
        file_path_folder_origin = os.path.dirname(path_file_origin)

        file_name_origin_without_ext = os.path.splitext(file_name_origin)[0]
        file_name_json = file_name_origin_without_ext + ".json"

        file_name_dest = make_reencode.get_file_name_dest(
            file_path_folder_origin, file_name_json, "video_metadata_"
        )
        json_path_file = os.path.join(path_folder_metadata, file_name_dest)
        dict_metadata = dict_inf_ffprobe["metadata"]
        with open(json_path_file, "w") as fout:
            json.dump(dict_metadata, fout, indent=2)


def step_create_report_filled(path_dir, path_file_report, video_extensions, reencode_plan='group'):
    """
    - create report with path_file video list
    - prefill reencode plan

    Args:
        path_dir (str): project folder path
        path_file_report (str): output path_file report
        video_extensions (list): list of video extensions to consider
    """

    list_file_selected = \
        video_report.get_list_path_video(path_dir, video_extensions)

    list_dict_inf_ffprobe = \
        video_report.get_list_dict_inf_ffprobe(list_file_selected)

    save_metadata_json_files(list_dict_inf_ffprobe, path_file_report)

    list_dict = video_report.gen_report(list_dict_inf_ffprobe)
    df = pd.DataFrame(list_dict)

    # sort path_file by natural human way
    df = df_sort_human(df, key_column_name="path_file")


    # prefill column video_resolution_to_change
    df = prefill.load(df, reencode_plan)

    # save
    df.to_excel(path_file_report, index=False)

    # Make backup. _origin
    create_report_backup(df=df,
                         path_file_report=path_file_report,
                         tag="1_origin")


def set_make_reencode(path_file_report, path_folder_videos_encoded):

    df = make_reencode.make_reencode(path_file_report,
                                     path_folder_videos_encoded)

    df.to_excel(path_file_report, index=False)

    # make backup
    create_report_backup(df=df,
                         path_file_report=path_file_report,
                         tag="2_reencode")

    print("\nReencode finished")


def set_correct_duration(path_file_report):

    df = correct_duration(path_file_report)
    df.to_excel(path_file_report, index=False)
    # make backup
    create_report_backup(df=df,
                         path_file_report=path_file_report,
                         tag="3_correct_duration")


def set_group_column(path_file_report):

    # update video_details with group_encode column

    df = pd.read_excel(path_file_report, engine="openpyxl")
    df = get_video_details_with_group(df)
    df.to_excel(path_file_report, index=False)
    print(
        f"File '{path_file_report}' was updated with "
        + "a guide column to fast join (group_encode) \n"
    )

    # Note: backup is not performed here as the
    #       grouping can be adjusted manually


def set_split_videos(
    path_file_report,
    mb_limit,
    path_folder_videos_splitted,
    duration_limit="00:00:00,00",
):

    df = pd.read_excel(path_file_report, engine="openpyxl")

    # backup group, after adjusted manually
    create_report_backup(df=df,
                         path_file_report=path_file_report,
                         tag="4_grouped")

    # Find for file_video too big and split them
    df = search_to_split_videos(
        df, mb_limit, path_folder_videos_splitted, duration_limit
    )

    df.to_excel(path_file_report, index=False)

    create_report_backup(df=df,
                         path_file_report=path_file_report,
                         tag="5_splited")


def set_join_videos(
    path_file_report,
    mb_limit,
    filename_output,
    path_folder_videos_joined,
    path_folder_videos_cache,
    duration_limit="00:00:00,00",
    start_index_output=1,
    activate_transition="false",
):

    transition_status = get_transition_effect_status(activate_transition)
    df = join_videos(
        path_file_report,
        mb_limit,
        filename_output,
        path_folder_videos_joined,
        path_folder_videos_cache,
        start_index_output,
        duration_limit,
        transition_status,
    )
    df.to_excel(path_file_report, index=False)

    # backup joined
    create_report_backup(df=df,
                         path_file_report=path_file_report,
                         tag="7_joined")


def set_path_file_report(path_dir):

    folder_name_normalized = get_folder_name_normalized(path_dir)
    folder_path_output_relative = os.path.join(
        "projects", "output_" + folder_name_normalized
    )
    ensure_folder_existence([folder_path_output_relative])
    path_file_report = os.path.join(folder_path_output_relative,
                                    "video_details.xlsx")
    return path_file_report


def set_path_folder_videos_encoded(path_dir):

    dir_name_normalize = get_folder_name_normalized(path_dir)
    folder_path_output_relative = os.path.join(
        "projects", "output_" + dir_name_normalize
    )
    path_folder_videos_encoded = os.path.join(
        folder_path_output_relative, "videos_encoded"
    )
    return path_folder_videos_encoded


def set_path_folder_videos_splitted(path_dir):

    dir_name_normalize = get_folder_name_normalized(path_dir)
    folder_path_output_relative = os.path.join(
        "projects", "output_" + dir_name_normalize
    )
    path_folder_videos_splitted = os.path.join(
        folder_path_output_relative, "videos_splitted"
    )
    return path_folder_videos_splitted


def set_path_folder_videos_joined(path_dir):

    dir_name_normalize = get_folder_name_normalized(path_dir)
    folder_path_output_relative = os.path.join(
        "projects", "output_" + dir_name_normalize
    )
    path_folder_videos_joined = os.path.join(
        folder_path_output_relative, "output_videos"
    )
    return path_folder_videos_joined


def set_path_folder_videos_cache(path_dir):

    dir_name_normalize = get_folder_name_normalized(path_dir)
    folder_path_output_relative = os.path.join(
        "projects", "output_" + dir_name_normalize
    )
    path_folder_videos_cache = \
        os.path.join(folder_path_output_relative, "cache")

    return path_folder_videos_cache


def get_config_data(path_file_config):
    """get default configuration data from file config.ini

    Returns:
        dict: config data
    """

    config_file = ConfigParser()
    config_file.read(path_file_config)
    default_config = dict(config_file["default"])
    return default_config


def config_update_data(path_file_config, variable_name, variable_value):

    config = ConfigParser()
    config.read(path_file_config)
    config.set("default", variable_name, variable_value)
    with open(path_file_config, "w+") as config_updated:
        config.write(config_updated)


def get_path_dir(path_dir):

    if path_dir is None:
        path_dir = input("\nPaste the folder link where are the video files: ")
    else:
        pass
    return path_dir


def get_path_file_report(path_file_report, path_dir):

    if path_file_report is None:
        path_file_report = set_path_file_report(path_dir)
    else:
        pass
    return path_file_report


def join_process_has_started(path_file_report):

    df = pd.read_excel(path_file_report, engine="openpyxl")
    if 'split_file_folder_origin' in df.columns:
        return True
    else:
        return False


def main():

    folder_script_path = get_folder_script_path()
    path_file_config = os.path.join(folder_script_path, "config.ini")
    config_data = get_config_data(path_file_config)
    size_per_file_mb = int(config_data["size_per_file_mb"])
    activate_transition = config_data["activate_transition"]
    duration_limit = config_data["duration_limit"]
    video_extensions = config_data["video_extensions"].split(",")
    start_index = int(config_data["start_index"])
    reencode_plan = config_data["reencode_plan"]
    path_file_report = None
    path_dir = None
    ensure_folder_existence(["projects"])
    while True:
        menu_answer = menu_ask()
        if menu_answer == 1:
            # create Dataframe of video details
            path_dir = get_path_dir(path_dir)
            path_file_report = set_path_file_report(path_dir)
            step_create_report_filled(path_dir,
                                      path_file_report,
                                      video_extensions,
                                      reencode_plan)

            print(
                "\nIf necessary, change the reencode plan in the column "
                + '"video_resolution_to_change"'
            )

            # break_point
            input("Type Enter to continue")
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

            print("start correcting the duration metadata")

            # correct videos duration
            set_correct_duration(path_file_report)
            print("\nDuration metadata corrected.")

            # break_point
            input(
                "\nType something to go to the main menu, "
                + 'and proceed to the "Group videos" process.'
            )

            clean_cmd()
            continue

        elif menu_answer == 3:

            # define variables
            path_dir = get_path_dir(path_dir)
            path_file_report = get_path_file_report(path_file_report, path_dir)

            path_folder_videos_splitted = \
                set_path_folder_videos_splitted(path_dir)

            ensure_folder_existence([path_folder_videos_splitted])

            path_folder_videos_joined = set_path_folder_videos_joined(path_dir)
            ensure_folder_existence([path_folder_videos_joined])

            filename_output = get_folder_name_normalized(path_dir)

            path_folder_videos_cache = set_path_folder_videos_cache(path_dir)
            ensure_folder_existence([path_folder_videos_cache])

            mb_limit = int(
                userpref_size_per_file_mb(size_per_file_mb, path_file_config)
            )
            duration_limit = get_duration_limit(duration_limit)

            if join_process_has_started(path_file_report):
                pass
            else:
                # establishes separation criteria for the join videos step
                set_group_column(path_file_report)

                # break_point
                # fmt: off
                input("Review the file and then type something to "
                      "start the process that look for videos that "
                      "are too big and should be splitted")

                set_split_videos(path_file_report,
                                 mb_limit,
                                 path_folder_videos_splitted,
                                 duration_limit)

            # join all videos
            set_join_videos(
                path_file_report,
                mb_limit,
                filename_output,
                path_folder_videos_joined,
                path_folder_videos_cache,
                duration_limit,
                start_index,
                activate_transition,
            )
            return
        else:
            return


if __name__ == "__main__":
    logging_config()
    main()
