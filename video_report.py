import logging
from ffprobe_micro import ffprobe
import video_tools
import os
import pandas as pd


def get_duration_ffprobe(dict_inf):

    d = {}
    file = dict_inf['format']['filename']
    try:
        duration_unformat = dict_inf['format']['duration']
        duration = video_tools.float_seconds_to_string(float_sec=float(duration_unformat))
    except:
        logging.error(f'Video without duration:\n{file}\n' +
                        'Please check and delete the file if ' +
                        'necessary')
        d['duration_str'] = ''
        d['duration_seconds'] = ''

    d['duration_str'] = duration
    d['duration_seconds'] = float(duration_unformat)
    return d


def get_video_codec(dict_inf, is_video):

    if is_video:
        video_codec = dict_inf['streams'][0]['codec_name']
    else:
        file = dict_inf['format']['filename']
        logging.error("File above don't have tag 'video' in " +
                      f'detail file:\n{file}')
        video_codec = ''
    return video_codec


def get_video_profile(dict_inf, is_video):

    if is_video:
        video_profile = dict_inf['streams'][0]['profile']
    else:
        file = dict_inf['format']['filename']
        logging.error("File above don't have tag 'video' in " +
                      f'detail file:\n{file}')
        video_profile = ''
    return video_profile


def get_video_resolution_height(dict_inf, is_video):

    if is_video:
        video_resolution_height = dict_inf['streams'][0]['height']
    else:
        file = dict_inf['format']['filename']
        logging.error("File above don't have tag 'video' in " +
                      f'detail file:\n{file}')
        video_resolution_height = ''
    return video_resolution_height


def get_video_resolution_width(dict_inf, is_video):

    if is_video:
        video_resolution_width = dict_inf['streams'][0]['width']
    else:
        file = dict_inf['format']['filename']
        logging.error("File above don't have tag 'video' in " +
                      f'detail file:\n{file}')
        video_resolution_width = ''
    return video_resolution_width


def get_video_bitrate(dict_inf, is_video):

    if is_video:
        video_bitrate = dict_inf['streams'][0]['bit_rate']
    else:
        file = dict_inf['format']['filename']
        logging.error("File above don't have tag 'video' in " +
                      f'detail file:\n{file}')
        video_bitrate = ''
    return int(video_bitrate)


def get_is_avc(dict_inf, is_video):

    file = dict_inf['format']['filename']
    if is_video:
        try:
            is_avc_str = dict_inf['streams'][0]['is_avc']
            if is_avc_str == 'true':
                is_avc = 1
            else:
                is_avc = 0
        except:
            logging.error('File above dont have tag "is_avc" in ' +
                          f'ffprobe output:\n{file}')
    else:
        logging.error("File above don't have tag 'video' in " +
                      f'detail file:\n{file}')
        is_avc = 0
    return is_avc


def get_audio_codec(dict_inf, is_audio):

    if is_audio:
        audio_codec = dict_inf['streams'][1]['codec_name']
    else:
        file = dict_inf['format']['filename']
        logging.error('File above dont have tag "audio" in ' +
                      f'detail file:\n{file}')
        audio_codec = ''
    return audio_codec


def get_list_path_video(path_dir, video_extensions):

    # To input more file video extension:
    #  https://dotwhat.net/type/video-movie-files

    tuple_video_extension = tuple(video_extensions)
    str_tuple_video_extension = ', '.join(tuple_video_extension)
    logging.info(f'Find for video with extension: {str_tuple_video_extension}')
    list_file_selected = []
    for root, _, files in os.walk(path_dir):

        for file in files:
            file_lower = file.lower()
            if file_lower.endswith(tuple_video_extension):
                logging.info(f'Selected file: {file}')
                path_file = os.path.join(root, file)
                list_file_selected.append(path_file)
            else:
                logging.info(f'Unselected file: {file}')
    return list_file_selected


def get_list_dict_inf_ffprobe(list_path_file):

    list_dict = []
    for file_selected in list_path_file:
        d={}
        d['path_file'] = file_selected
        # generate raw metadata
        dict_inf_ffprobe = ffprobe(file_selected).get_output_as_dict()
        d['metadata'] = dict_inf_ffprobe
        list_dict.append(d)
    return list_dict


def gen_report(list_dict_inf_ffprobe):

    list_dict = []
    for dict_file in list_dict_inf_ffprobe:

        path_file = dict_file['path_file']
        dict_inf_ffprobe = dict_file['metadata']
        # parse data
        duration_dict = get_duration_ffprobe(dict_inf=dict_inf_ffprobe)
        duration = duration_dict['duration_str']
        duration_seconds = duration_dict['duration_seconds']
        total_bitrate = int(dict_inf_ffprobe['format']['bit_rate'])

        is_video = dict_inf_ffprobe['streams'][0]['codec_type'] == 'video'
        if is_video:
            video_codec = get_video_codec(dict_inf_ffprobe, is_video)
            video_profile = get_video_profile(dict_inf_ffprobe, is_video)
            video_resolution_height = \
                get_video_resolution_height(dict_inf_ffprobe, is_video)
            video_resolution_width = \
                get_video_resolution_width(dict_inf_ffprobe, is_video)
            video_bitrate = get_video_bitrate(dict_inf_ffprobe, is_video)
            is_avc = get_is_avc(dict_inf_ffprobe, is_video)
        else:
            logging.error("File above don't have tag 'video' in " +
                          f'detail file:\n{path_file}')
            continue

        has_audio = dict_inf_ffprobe['streams'][1]['codec_type'] == 'audio'
        if has_audio:
            audio_codec = get_audio_codec(dict_inf_ffprobe, has_audio)
        else:
            audio_codec = ''

        # generate dict
        d = {}
        d['duration'] = duration
        d['duration_seconds'] = duration_seconds
        d['total_bitrate'] = total_bitrate
        d['video_codec'] = video_codec
        d['video_profile'] = video_profile
        d['video_resolution_height'] = video_resolution_height
        d['video_resolution_width'] = video_resolution_width
        d['video_bitrate'] = video_bitrate
        d['is_avc'] = is_avc
        d['audio_codec'] = audio_codec
        d['file_size'] = os.path.getsize(path_file)
        d['path_file'] = path_file
        d['file_path_folder'] = os.path.dirname(path_file)
        d['file_name'] = os.path.split(path_file)[1]
        list_dict.append(d)

    return list_dict
