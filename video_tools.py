"""
    Create by: apenasrr
    Source: https://github.com/apenasrr/mass_videojoin
"""

import pandas as pd
import os
import logging
import sys
import re
import glob
import subprocess


def change_width_height_mp4(path_file_video_origin, size_height, 
                            size_width, path_file_video_dest):
    """
    More info: https://www.reck.dk/ffmpeg-autoscale-on-height-or-width/
    :input: size_height: Eg. 480 or 720 or 1080...
    """

    logging.info(f'Changing height to {size_height}: {path_file_video_origin}')
    size_height = str(size_height)
    stringa = f'ffmpeg -y -i "{path_file_video_origin}" -vf scale={size_width}:{size_height},setsar=1:1 -c:v libx264 -c:a copy "{path_file_video_dest}"'
    os.system(stringa)
    logging.info(f'Done')


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
    logging.info(f'Done')

    
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
    # file_name_without_extension = os.path.splitext(largefile_path)[0]
    file_size = os.stat(largefile_path).st_size
    limit_size = mb_limit * 1024**2
    slices_qt = file_size//limit_size + 1
    
    video_duration_sec = get_length(largefile_path)
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
        
        stringa = \
            f'ffmpeg -i "{largefile_path}" ' +\
            f'{time_start_string}' +\
            f'{duration_string}' +\
            f'-c copy "{filepath_output}"'
        os.system(stringa)
        
    # return a list with every filepath created
    return list_filepath_output
    
    
def join_mp4(list_file_path, file_name_output):

    def exclude_temp_files():

        dir_ts = r'ts/*'
        r = glob.glob(dir_ts)
        for i in r:
            os.remove(i)
            
    exclude_temp_files()
    
    # copy to .ts
    list_path_file_name_ts = []
    logging.info('Convert files to TS: ')
    for index, file_path in enumerate(list_file_path):
        logging.info(f'"{index+1}.ts" from "{file_path}"')
        path_file_name_ts = f'ts/{index+1}.ts'
        os.system("ffmpeg -i " + '"' + file_path + '"' + \
                  " -c copy -bsf:v h264_mp4toannexb -f mpegts " + \
                  f'ts/{index+1}.ts')
        list_path_file_name_ts.append(path_file_name_ts)
    
    
    # list_path_file_name_ts = ['ts/1.ts', 'ts/2.ts', 'ts/3.ts', 'ts/4.ts']
    logging.info('\n')
    logging.info('Join files from TS to MP4: ')
    stringa = "ffmpeg -i \"concat:"
    index_final = len(list_path_file_name_ts)-1
    for index, path_file_name_ts in enumerate(list_path_file_name_ts):
        stringa += path_file_name_ts
        if index != index_final:
            stringa += "|"
        else:
            stringa += f"\" -c copy  -bsf:a aac_adtstoasc " + \
                       f"{file_name_output}"
    
    os.system(stringa)
    
    for path_file_name_ts in list_path_file_name_ts:
        os.remove(path_file_name_ts)


def get_video_details(filepath):

    file_temp = 'detail.txt'
    tmpf = open(file_temp,'r', encoding='utf-8')
    os.system("ffmpeg -i \"%s\" 2> %s" % (filepath, file_temp))
    lines = tmpf.readlines()
    tmpf.close()
    metadata = {}
    for l in lines:
        l = l.strip()
        if l.startswith('Duration'):
            metadata['duration'] = re.search('Duration: (.*?),', l).group(0).split(':',1)[1].strip(' ,')
            metadata['bitrate'] = re.search("bitrate: (\d+ kb/s)", l).group(0).split(':')[1].strip()
        if l.startswith('Stream #0:0'):
            metadata['video'] = {}
            metadata['video']['codec'], metadata['video']['profile'] = \
                [e.strip(' ,()') for e in re.search('Video: (.*? \(.*?\)),? ', l).group(0).split(':')[1].split('(')]
            metadata['video']['resolution'] = re.search('([1-9]\d+x\d+)', l).group(1)
            metadata['video']['bitrate'] = re.search('(\d+ kb/s)', l).group(1)
            metadata['video']['fps'] = re.search('(\d+ fps)', l).group(1)
        if l.startswith('Stream #0:1'):
            metadata['audio'] = {}
            metadata['audio']['codec'] = re.search('Audio: (.*?) ', l).group(1)
            metadata['audio']['frequency'] = re.search(', (.*? Hz),', l).group(1)
            metadata['audio']['bitrate'] = re.search(', (\d+ kb/s)', l).group(1)
    return metadata
    