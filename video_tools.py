"""
    Create by: apenasrr
    Source: https://github.com/apenasrr/mass_videojoin
"""

import glob
import logging
import os
import subprocess
from datetime import timedelta

from ffprobe_micro import ffprobe
from utils_mass_videojoin import get_file_name_dest


def get_video_resolution(file_path):
    """[summary]

    Args:
        file_path (str): absolute video path file

    Returns:
        dict: keys: height, width]
    """

    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=width,height",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            file_path,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    result_stdout = str(result.stdout)
    list_height_width = result_stdout.replace("b'", "").split(r"\r\n")
    height = list_height_width[0]
    width = list_height_width[1]
    resolution = {}
    resolution["height"] = height
    resolution["width"] = width
    return resolution


def get_maxrate(size_height):
    """
    Maxrate equivalent to total bitrate 1000 kbps for 720p, with 128 kbps audio
    """
    # 720, v872, a128

    # guide variables
    constant_size_height = 720
    constant_video_maxrate = 872
    constant_audio_quality = 128
    # fmt: off
    constant_video_quality = \
        (constant_size_height ** 2 * (16 / 9)) / constant_video_maxrate

    # maxrate calc
    density = int(size_height) ** 2 * (16 / 9)
    maxrate = density / constant_video_quality + constant_audio_quality
    maxrate = int(maxrate)

    return maxrate


def convert_mp4_wo_reencode(path_file_video_origin, path_file_video_dest):

    logging.info(f"Convert video extension without reencode: {path_file_video_origin}")

    stringa = (
        f'ffmpeg -y -i "{path_file_video_origin}" '
        + "-vcodec copy "
        + f'-acodec copy "{path_file_video_dest}"'
    )

    os.system(stringa)
    logging.info("Done")


def change_width_height_mp4(
    path_file_video_origin, size_height, size_width, path_file_video_dest
):
    """
    More info: https://www.reck.dk/ffmpeg-autoscale-on-height-or-width/
    :input: size_height: Eg. 480 or 720 or 1080...
    """

    logging.info(f"Changing height to {size_height}: {path_file_video_origin}")

    size_height = str(size_height)

    # for fix audio codec to aac | https://trac.ffmpeg.org/wiki/Encode/AAC
    stringa = (
        f'ffmpeg -y -i "{path_file_video_origin}" '
        + f'-vf "scale=w={size_width}:h={size_height}:'
        + "force_original_aspect_ratio=1,"
        + f'pad={size_width}:{size_height}:(ow-iw)/2:(oh-ih)/2" '
        + "-c:v libx264 -crf 18 -maxrate 2.5M -bufsize 4M -preset ultrafast -flags +global_header "
        + "-pix_fmt yuv420p -profile:v baseline -tune zerolatency -movflags +faststart "
        + f'-c:a aac "{path_file_video_dest}"'
    )

    os.system(stringa)
    logging.info("Done")


def get_cmd_convert_streaming(path_file_video_origin, path_file_video_dest):
    """
    Inf.: https://trac.ffmpeg.org/wiki/StreamingGuide
    """

    # TODO: If  AAC AUDIO_CODEC, set -c:a as 'copy'

    stringa = (
        f'ffmpeg -y -i "{path_file_video_origin}" '
        + "-c:v libx264 -crf 18 -maxrate 2.5M -bufsize 4M "
        + "-preset ultrafast "
        + "-flags +global_header "
        + "-pix_fmt yuv420p "
        + "-profile:v baseline "
        + "-tune zerolatency "
        + "-movflags +faststart "
        + "-c:a aac "
        + f'"{path_file_video_dest}"'
    )
    return stringa


def convert_streaming(path_file_video_origin, path_file_video_dest):

    stringa = get_cmd_convert_streaming(
                path_file_video_origin, path_file_video_dest
                )

    os.system(stringa)
    logging.info("Done")

#TODO: create function to convert to mp4 without reencode. Case of .ts from tubedigger


def split_mp4(
    largefile_path,
    recoil,
    output_folder_path,
    mb_limit=0,
    original_video_duration_sec=0,
):
    """
    Split video without reencode
    :input: recoil: Int. Seconds add to initial 'part 2' to prevent lost frames
    :input: time_split_sec: Int. Moment in seconds where the video must be cut
    :input: mb_limit: Int. File size limit per slice in megabyte.
    :input: original_video_duration_sec: optional. Int. Duration of origin video
    """

    # The second slice needs to start seconds before cutting
    # Without re-encoding, you can only cut videos at "key frames".
    # Frames in between key frames don't carry enough information on their
    # own to build a complete image.
    # See: trac.ffmpeg.org/wiki/Seeking#Seekingwhiledoingacodeccopy

    def get_length(filename):
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                filename,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        return float(result.stdout)

    if mb_limit == 0:
        print("split_mp4: Inform variable mb_limit.")
        return False

    file_folder = file_name = os.path.split(largefile_path)[0]
    file_name = os.path.split(largefile_path)[1]

    file_name_hashed = get_file_name_dest(
        file_folder_origin=file_folder,
        file_name_origin=file_name,
        prefix="split_",
    )

    file_name_without_extension = os.path.splitext(file_name_hashed)[0]
    file_size = os.stat(largefile_path).st_size
    limit_size = mb_limit * 1024 ** 2
    slices_qt = file_size // limit_size + 1

    if original_video_duration_sec == 0:
        original_video_duration_sec = get_length(largefile_path)
    video_duration_sec = original_video_duration_sec + (
        (slices_qt - 1) * recoil
    )

    duration_per_split_sec = int(video_duration_sec / slices_qt)

    list_filepath_output = []
    for index in range(slices_qt):
        number_file = index + 1
        if index == 0:
            time_start_string = ""
        else:
            time_start = (duration_per_split_sec - recoil) * (index)
            time_start_string = f"-ss {time_start} "

        if index + 1 != slices_qt:
            duration_string = f"-t {duration_per_split_sec} "
        else:
            duration_string = ""

        filename_output = (
            f"{file_name_without_extension}-%03d.mp4" % number_file
        )
        print(filename_output)
        filepath_output = os.path.join(output_folder_path, filename_output)

        # save list with filepath_output
        list_filepath_output.append(filepath_output)

        stringa = (
            f'ffmpeg -i "{largefile_path}" '
            + f"{time_start_string}"
            + f"{duration_string}"
            + f'-c copy "{filepath_output}"'
        )
        os.system(stringa)

    # return a list with every filepath created
    return list_filepath_output


def timedelta_to_string(timestamp):

    microsec = timedelta(microseconds=timestamp.microseconds)
    timestamp = timestamp - microsec
    hou, min_full = divmod(timestamp.seconds, 3600)
    min, sec = divmod(min_full, 60)
    str_microsec = int(microsec.microseconds / 10000)
    timestamp = "%02d:%02d:%02d.%02d" % (hou, min, sec, str_microsec)

    return timestamp


def float_seconds_to_string(float_sec):
    """Convert seconds in float, to string in format hh:mm:ss

    Args:
        float_sec (float): Seconds

    Returns:
        String: Time in format hh:mm:ss
    """

    timedelta_seconds = timedelta(seconds=float_sec)

    # format string: hh:mm:ss
    string_timedelta = timedelta_to_string(timestamp=timedelta_seconds)
    return string_timedelta


def float_seconds_from_string(str_hh_mm_ss_ms):
    """Convert to seconds in float, from string in format hh:mm:ss

    Args:
        string_timedelta (str): format hh:mm:ss.ms

    Returns:
        Float: timedelta in seconds
    """

    hr, min, sec = map(float, str_hh_mm_ss_ms.split(":"))
    float_sec_timedelta = sec + min * 60 + hr * 60 * 60

    return float_sec_timedelta


def get_duration_ffprobe(dict_inf):

    d = {}
    try:
        file = dict_inf["format"]["filename"]
    except Exception as e:
        print(f"\n{dict_inf}")
        print(f"\n{e}")
        return False
    try:
        duration_unformat = dict_inf["format"]["duration"]
        duration = float_seconds_to_string(float_sec=float(duration_unformat))
        d["duration_str"] = duration
        d["duration_seconds"] = float(duration_unformat)
    except:
        logging.error(
            f"Video without duration:\n{file}\n"
            + "Please check and delete the file if "
            + "necessary"
        )
        d["duration_str"] = ""
        d["duration_seconds"] = ""

    return d


def get_duration(file_path):

    dict_inf_ffprobe = ffprobe(file_path).get_output_as_dict()
    duration_dict = get_duration_ffprobe(dict_inf=dict_inf_ffprobe)
    duration_seconds = duration_dict["duration_seconds"]

    return float(duration_seconds)


def exclude_temp_files(path_folder_cache):

    dir_ts = os.path.join(path_folder_cache, "*")
    r = glob.glob(dir_ts)
    for i in r:
        os.remove(i)


def get_dict_video_duration(path_file, file_path_origin):

    float_duration = get_duration(path_file)
    string_duration = float_seconds_to_string(float_duration)

    dict_videos_duration = {}
    dict_videos_duration["file_path_origin"] = file_path_origin
    dict_videos_duration["duration_real"] = string_duration
    return dict_videos_duration


def convert_to_ts(list_file_path, output_path_folder):
    """convert a video list to ts

    Args:
        list_file_path (list): list of path_file with mp4 extension
        output_path_folder (string): output path folder
    """

    # copy to .ts
    list_path_file_name_ts = []
    logging.info("Convert files to TS: ")

    exclude_temp_files(output_path_folder)

    list_dict_videos_duration = []

    for index, file_path in enumerate(list_file_path):

        file_name_ts = f"{index+1}.ts"
        path_file_name_ts = os.path.join(output_path_folder, file_name_ts)
        logging.info(
            f'"{index+1}.ts" from "{file_path}", to "{path_file_name_ts}"'
        )
        os.system(
            "ffmpeg -i "
            + '"'
            + file_path
            + '"'
            + " -c copy -bsf:v h264_mp4toannexb -f mpegts "
            + path_file_name_ts
        )

        dict_videos_duration = get_dict_video_duration(
            path_file_name_ts, file_path
        )
        list_dict_videos_duration.append(dict_videos_duration)

        list_path_file_name_ts.append(path_file_name_ts)

    dict_return = {
        "list_dict_videos_duration": list_dict_videos_duration,
        "list_path_file_name_ts": list_path_file_name_ts,
    }
    return dict_return


def join_mp4(list_file_path, file_name_output, path_folder_cache):
    """join a list of video path_file with mp4 extension

    Args:
         list_file_path (list): list of path_file with mp4 extension
         file_name_output (string): filename output
         path_folder_cache (string): path folder cache to temp alloc ts videos
    Returns:
         list: list of dicts:
                file_path_origin (string): file_path of original video,
                duration_real (string): real video duration,
                                        format="hh:mm:ss.ms"
    """

    convert_ts_return = convert_to_ts(list_file_path, path_folder_cache)
    list_dict_videos_duration = convert_ts_return["list_dict_videos_duration"]
    list_path_file_name_ts = convert_ts_return["list_path_file_name_ts"]

    logging.info("\n")
    logging.info("Join files from TS to MP4: ")
    stringa = 'ffmpeg -analyzeduration 20M -probesize 20M -i "concat:'
    index_final = len(list_path_file_name_ts) - 1
    for index, path_file_name_ts in enumerate(list_path_file_name_ts):
        stringa += path_file_name_ts
        if index != index_final:
            stringa += "|"
        else:
            stringa += (
                '" -c copy -flags +global_header -pix_fmt yuv420p '
                "-movflags +faststart -bsf:a aac_adtstoasc "
                f"{file_name_output}"
            )

    os.system(stringa)
    exclude_temp_files(path_folder_cache)
    return list_dict_videos_duration
