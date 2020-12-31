"""
    Create by: apenasrr
    Source: https://github.com/apenasrr/mass_videojoin
"""

import os
import logging
import re
import glob
import subprocess
from datetime import timedelta


def get_maxrate(size_height):
    """
    Maxrate equivalent to total bitrate 1000 kbps for 720p, with 128 kbps audio
    """
    # 720, v872, a128

    # guide variables
    constant_size_height = 720
    constant_video_maxrate = 872
    constant_audio_quality = 128
    constant_video_quality = \
        (constant_size_height**2*(16/9))/constant_video_maxrate

    # maxrate calc
    density = int(size_height)**2*(16/9)
    maxrate = density/constant_video_quality+constant_audio_quality
    maxrate = int(maxrate)

    return maxrate


def change_width_height_mp4(path_file_video_origin, size_height,
                            size_width, path_file_video_dest):
    """
    More info: https://www.reck.dk/ffmpeg-autoscale-on-height-or-width/
    :input: size_height: Eg. 480 or 720 or 1080...
    """
    # TODO include change of audio codec and video codec. optional argument. \
    # Useful to change for the 'main codecs' of the loot

    logging.info(f'Changing height to {size_height}: {path_file_video_origin}')

    maxrate = get_maxrate(size_height)
    str_bufsize = str(maxrate*2)
    str_maxrate = str(maxrate)
    size_height = str(size_height)

    # for fix audio codec to aac | https://trac.ffmpeg.org/wiki/Encode/AAC
    stringa = f'ffmpeg -y -i "{path_file_video_origin}" ' + \
              f'-vf scale={size_width}:{size_height},setsar=1:1 ' + \
              f'-c:v libx264 -maxrate {str_maxrate}k ' + \
              f'-bufsize {str_bufsize}k -c:a aac "{path_file_video_dest}"'

    os.system(stringa)
    logging.info('Done')


def change_height_width_mp4(path_file_video_origin, size_height,
                            size_width, path_file_video_dest):
    """
    More info: https://www.reck.dk/ffmpeg-autoscale-on-height-or-width/
    :input: size_height: Eg. 480, 720, 1080...
    :input: size_width: Eg. 854, 1280, 1920...
    """

    logging.info(f'Changing height to {size_height}: {path_file_video_origin}')
    size_height = str(size_height)
    stringa = f'ffmpeg -y -i "{path_file_video_origin}" -vf ' + \
              f'scale={size_width}:{size_height},setsar=1:1 -c:v libx264 ' + \
              f'-c:a copy "{path_file_video_dest}"'
    os.system(stringa)
    logging.info('Done')


def split_mp4(largefile_path, recoil, output_folder_path, mb_limit=0):
    """
    Split video without reencode
    :input: recoil: Int. Seconds add to initial 'part 2' to prevent lost frames
    :input: time_split_sec: Int. Moment in seconds where the video must be cut
    :input: mb_limit: Int. File size limit per slice in megabyte.
    """

    # The second slice needs to start seconds before cutting
    # Without re-encoding, you can only cut videos at "key frames".
    # Frames in between key frames don't carry enough information on their
    # own to build a complete image.
    # See: trac.ffmpeg.org/wiki/Seeking#Seekingwhiledoingacodeccopy

    def get_length(filename):
        result = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                                 "format=duration", "-of",
                                 "default=noprint_wrappers=1:nokey=1",
                                 filename],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)

        return float(result.stdout)

    if mb_limit == 0:
        print('split_mp4: Inform variable mb_limit.')
        return False

    file_name = os.path.split(largefile_path)[1]
    file_name_without_extension = os.path.splitext(file_name)[0]
    file_size = os.stat(largefile_path).st_size
    limit_size = mb_limit * 1024**2
    slices_qt = file_size//limit_size + 1
    original_video_duration_sec = get_length(largefile_path)
    video_duration_sec = original_video_duration_sec + ((slices_qt)*recoil)
    duration_per_split_sec = int(video_duration_sec/slices_qt)

    list_filepath_output = []
    for index in range(slices_qt):
        number_file = index+1
        if index == 0:
            time_start_string = ''
        else:
            time_start = (duration_per_split_sec - recoil) * (index)
            time_start_string = f'-ss {time_start} '

        if index + 1 != slices_qt:
            duration_string = f'-t {duration_per_split_sec} '
        else:
            duration_string = ''

        filename_output = \
            f'{file_name_without_extension}-%03d.mp4' % number_file
        print(filename_output)
        filepath_output = os.path.join(output_folder_path, filename_output)

        # save list with filepath_output
        list_filepath_output.append(filepath_output)

        stringa = f'ffmpeg -i "{largefile_path}" ' +\
            f'{time_start_string}' +\
            f'{duration_string}' +\
            f'-c copy "{filepath_output}"'
        os.system(stringa)

    # return a list with every filepath created
    return list_filepath_output


def timedelta_to_string(timestamp):

    microsec = timedelta(microseconds=timestamp.microseconds)
    timestamp = timestamp - microsec
    hou, min_full = divmod(timestamp.seconds, 3600)
    min, sec = divmod(min_full, 60)
    str_microsec = int(microsec.microseconds/10000)
    timestamp = '%02d:%02d:%02d.%02d' % (hou, min, sec, str_microsec)

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


def join_mp4(list_file_path, file_name_output):
    """join a list of video path_file with mp4 extension

    Args:
         list_file_path (list): list of path_file with mp4 extension
         file_name_output (string): filename output
    Returns:
         list: list of dicts:
                file_path_origin (string): file_path of original video,
                duration_real (string): real video duration, format hh:mm:ss.
    """

    def exclude_temp_files(folder_script_path):

        dir_ts = os.path.join(folder_script_path, 'ts', '*')
        r = glob.glob(dir_ts)
        for i in r:
            os.remove(i)

    def get_duration(file_path):

        result = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                                 "format=duration", "-of",
                                 "default=noprint_wrappers=1:nokey=1",
                                 file_path],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        return float(result.stdout)

    def get_dict_videos_duration(path_file_name_ts):

        float_duration = get_duration(path_file_name_ts)
        string_duration = \
            float_seconds_to_string(float_duration)

        dict_videos_duration = {}
        dict_videos_duration['file_path_origin'] = file_path
        dict_videos_duration['duration_real'] = string_duration
        return dict_videos_duration

    # copy to .ts
    list_path_file_name_ts = []
    logging.info('Convert files to TS: ')

    folder_script_path_relative = os.path.dirname(__file__)
    folder_script_path = os.path.realpath(folder_script_path_relative)
    exclude_temp_files(folder_script_path)

    list_dict_videos_duration = []

    for index, file_path in enumerate(list_file_path):
        logging.info(f'"{index+1}.ts" from "{file_path}"')
        file_name_ts = f'{index+1}.ts'
        path_file_name_ts = os.path.join(
            folder_script_path, 'ts', file_name_ts)
        os.system("ffmpeg -i " + '"' + file_path + '"' +
                  " -c copy -bsf:v h264_mp4toannexb -f mpegts " +
                  path_file_name_ts)

        dict_videos_duration = \
            get_dict_videos_duration(path_file_name_ts)
        list_dict_videos_duration.append(dict_videos_duration)

        list_path_file_name_ts.append(path_file_name_ts)

    logging.info('\n')
    logging.info('Join files from TS to MP4: ')
    stringa = "ffmpeg -i \"concat:"
    index_final = len(list_path_file_name_ts)-1
    for index, path_file_name_ts in enumerate(list_path_file_name_ts):
        stringa += path_file_name_ts
        if index != index_final:
            stringa += "|"
        else:
            stringa += "\" -c copy  -bsf:a aac_adtstoasc " + \
                f"{file_name_output}"

    os.system(stringa)
    exclude_temp_files(folder_script_path)
    return list_dict_videos_duration


def get_video_details(filepath):

    folder_script_path_relative = os.path.dirname(__file__)
    folder_script_path = os.path.realpath(folder_script_path_relative)
    file_temp = os.path.join(folder_script_path, 'detail.txt')

    # create temp file
    open(file_temp, 'a').close()

    # open temp file
    tmpf = open(file_temp, 'r', encoding='utf-8')

    # fill temp file with video metadata using ffmpeg
    os.system("ffmpeg -i \"%s\" 2> %s" % (filepath, file_temp))

    # read temp file
    lines = tmpf.readlines()

    # close temp file
    tmpf.close()

    # delete temp file
    # os.remove(file_temp)

    # parse content
    metadata = {}
    for l in lines:
        l = l.strip()
        if l.startswith('Duration'):
            metadata['duration'] = re.search(
                'Duration: (.*?),', l).group(0).split(':', 1)[1].strip(' ,')
            try:
                metadata['bitrate'] = \
                    re.search(
                        "bitrate: (\d+ kb/s)",
                        l).group(0).split(':')[1].strip()
            except:
                # .webm videos with encode Lavf56.40.101,
                # may has 'Duration: N/A, start: -0.007000, bitrate: N/A'
                metadata['bitrate'] = ''

        if 'Video: ' in l:
            if 'video' in metadata:
                # if has another line of 'Video: ', possible of screens,
                #  ignore. e.g:
                #   Stream #0:2: Video: bmp, bgra, 640x360, 90k tbr, 90k tbn,
                #    90k tbc (attached pic)
                continue
            metadata['video'] = {}
            metadata['video']['codec'], metadata['video']['profile'] = \
                [e.strip(' ,()') for e in re.search(
                    'Video: (.*? \(.*?\)),? ',
                    l).group(0).split(':')[1].split('(')]

            metadata['video']['resolution'] = re.search(
                '([1-9]\d+x\d+)', l).group(1)

            try:
                metadata['video']['bitrate'] = re.search(
                    '(\d+ kb/s)', l).group(1)
            except:
                # .webm videos with encode Lavf56.40.101,
                #  may has 'Video:
                #   vp9 (Profile 0), yuv420p(tv, bt709/unknown/unknown)'
                metadata['video']['bitrate'] = ''
            try:
                metadata['video']['fps'] = re.search('(\d+ fps)', l).group(1)
            except:
                metadata['video']['fps'] = ''

        if 'Audio: ' in l:
            metadata['audio'] = {}
            metadata['audio']['codec'] = re.search('Audio: (.*?) ', l).group(1)
            metadata['audio']['frequency'] = re.search(
                ', (.*? Hz),', l).group(1)
            try:
                metadata['audio']['bitrate'] = re.search(
                    ', (\d+ kb/s)', l).group(1)
            except:
                # .webm videos with encode Lavf56.40.101,
                #  may has 'Audio: opus, 48000 Hz, stereo, fltp (default)'
                metadata['audio']['bitrate'] = ''

    if 'audio' not in metadata:
        metadata['audio'] = {}
        metadata['audio']['codec'] = ''
        metadata['audio']['frequency'] = ''
        metadata['audio']['bitrate'] = ''

    return metadata
