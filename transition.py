from video_tools import change_width_height_mp4, get_video_resolution
import logging
import os


def logging_config():

    logfilename = 'log-' + 'transition' + '.txt'
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


def get_folder_script_path():

    folder_script_path_relative = os.path.dirname(__file__)
    folder_script_path = os.path.realpath(folder_script_path_relative)

    return folder_script_path


def get_path_file_output(path_file_max_transition, resolution):

    folder_script_path = get_folder_script_path()
    path_dir = 'transition'
    file_name = os.path.basename(path_file_max_transition)
    file_name_without_extension = os.path.splitext(file_name)[0]
    file_name_output_pre = file_name_without_extension.split('_')[0]
    file_name_output = file_name_output_pre + "_" + resolution + '.mp4'

    path_file_output = os.path.join(folder_script_path, path_dir,
                                    file_name_output)

    return path_file_output


def get_ratio_standard(width, height):
    """find out if resolution are more closer to 16x9 or 4x3

    Args:
        width (str):
        height (str):
    Returns:
        (str): '16x9' or '4x3'
    """

    width = int(width)
    height = int(height)

    division_16_9 = 16 / 9
    division_4_3 = 4 / 3

    division_video_resolution = width / height

    dif_16_9 = abs(division_video_resolution - division_16_9)
    dif_4_3 = abs(division_video_resolution - division_4_3)
    min_dif = min(dif_16_9, dif_4_3)
    if min_dif == dif_16_9:
        return '16x9'
    else:
        return '4x3'


def get_resolutions_where_ratio(resolution_ratio):
    """from a ratio, get the list of resolution of archived video transitions

    Args:
        resolution_ratio (str): options: 16x9 or 4x3

    Returns:
        list: transition resolutions already archived
    """

    dict_transition_resolution = get_dict_transition_resolution()
    list_transition_standard = []
    for transition_resolution in list(dict_transition_resolution.keys()):
        width, height = transition_resolution.split('x')
        ratio_standard = get_ratio_standard(width, height)
        if ratio_standard == resolution_ratio:
            list_transition_standard.append(transition_resolution)
    if len(list_transition_standard) == 0:
        raise Exception('Missing transition video ' +
                        f'for ratio {resolution_ratio}.\n' +
                        'Please save a transition video ' +
                        'with this aspect ratio.')
    return list_transition_standard


def find_max_resolution(ratio):

    def get_height(resolution):
        return int(resolution.split('x')[1])

    list_transition_ratio_16x9 = get_resolutions_where_ratio(ratio)
    list_height = list(map(get_height, list_transition_ratio_16x9))
    max_height = max(list_height)
    max_index = list_height.index(max_height)
    max_resolution = list_transition_ratio_16x9[max_index]
    return max_resolution


def get_dict_path_file_transition_max_resiution():

    def get_path_file_transition_max_resiution(ratio):

        dict_transition_resolution_filed = get_dict_transition_resolution()
        max_resolution = find_max_resolution(ratio)
        path_file_transition_max_resiution = \
            dict_transition_resolution_filed[max_resolution]
        return path_file_transition_max_resiution

    dict_path_file_transition_max_resiution = {}
    ratios = ['16x9', '4x3']
    for ratio in ratios:
        dict_path_file_transition_max_resiution[ratio] = \
            get_path_file_transition_max_resiution(ratio)
    return dict_path_file_transition_max_resiution


def get_video_resolution_format(file_path):

    resolution = get_video_resolution(file_path)
    height = resolution['height']
    width = resolution['width']
    resolutions_format = f'{height}x{width}'
    return resolutions_format


def get_list_transition_path_file():

    folder_script_path = get_folder_script_path()
    path_dir = os.path.join(folder_script_path, 'transition')
    list_path_file = []
    for root, _, files in os.walk(path_dir):
        for file in files:
            file_lower = file.lower()
            if file_lower.endswith('.mp4'):
                path_file = os.path.join(root, file)
                list_path_file.append(path_file)

    return list_path_file


def get_dict_transition_resolution():
    """

    Returns:
        dict: keys=Resolution in string format: heightxwidth. e.g.: 1280x720
              value=transition Video file path
    """

    list_transition_path_file = get_list_transition_path_file()
    if len(list_transition_path_file) == 0:
        raise Exception("There is no transition video archived " +
                        "in the transition folder")
    dict_transition_resolutions = {}
    for transition_path_file in list_transition_path_file:
        resolutions = get_video_resolution_format(transition_path_file)
        if resolutions not in dict_transition_resolutions:
            dict_transition_resolutions[resolutions] = transition_path_file
    return dict_transition_resolutions


def check_transition_resolution(list_video_path_file):
    """Check if there are missing transitions.
       If any, generate them based on the transition
       from the same ratio with higher resolution

    Args:
        list_video_path_file (list): list of absolute path file of videos
    """

    # dict. Key: resolutions from archived transition. Value: file_path
    dict_transition_resolution_archived = get_dict_transition_resolution()

    # set of resolution from working videos
    list_video_resolutions = []
    for video_path_file in list_video_path_file:
        video_resolution = get_video_resolution_format(video_path_file)
        list_video_resolutions.append(video_resolution)
    set_video_resolutions = set(list_video_resolutions)

    # find out which transition resolutions are not archived
    resolutions_missing = []
    for video_resolution in set_video_resolutions:
        if video_resolution not in dict_transition_resolution_archived.keys():
            resolutions_missing.append(video_resolution)

    if len(resolutions_missing) == 0:
        return

    logging.info(f'Generate transition to resolutions: {resolutions_missing}')
    dict_path_file_transition_max_resiution = \
        get_dict_path_file_transition_max_resiution()

    # gera as transições faltantes,
    #  com base na transição de mesmo ratio com maior resolução
    for resolution in resolutions_missing:

        width, height = resolution.split('x')
        ratio = get_ratio_standard(width, height)
        path_file_transition_max_resolution = \
            dict_path_file_transition_max_resiution[ratio]

        path_file_output = \
            get_path_file_output(path_file_transition_max_resolution,
                                 resolution)

        change_width_height_mp4(
            path_file_video_origin=path_file_transition_max_resolution,
            size_height=height, size_width=width,
            path_file_video_dest=path_file_output)
