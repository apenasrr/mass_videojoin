"""
    Create by: apenasrr
    Source: https://github.com/apenasrr/mass_videojoin
"""

import os
import pandas as pd
import datetime
import logging
from video_tools import change_width_height_mp4, get_video_details, \
                        join_mp4, split_mp4
from config_handler import handle_config_file


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
    logger = logging.getLogger(__name__)


def clean_cmd():
    
    clear = lambda: os.system('cls')
    clear()
    
    
def gen_report(path_dir):

    # TODO improve sort file as windows explorer method
    ## https://stackoverflow.com/questions/59436556/python-how-to-get-the-same-sorting-order-as-windows-file-name-sorting

    # TODO input more file video extension:
    ## https://dotwhat.net/type/video-movie-files
    
    l=[]
    for root, dirs, files in os.walk(path_dir):
        for file in files:
            file_lower = file.lower()
            if file_lower.endswith((".mp4", ".webm")):
                print(file)
                
                path_file = os.path.join(root, file)
                dict_inf = get_video_details(path_file)
                (mode, ino, dev, nlink, uid, 
                gid, size, atime, mtime, ctime) = os.stat(path_file) 
                mtime = datetime.datetime.fromtimestamp(mtime)
                d={}
                d['mtime']=mtime
                d['file_folder'] = root
                d['file_name'] = file
                d['file_size'] = os.path.getsize(path_file)
                
                try:
                    d['duration'] = dict_inf['duration']
                except:
                    # sign of corrupt video file
                    logging.error(f'File corrupt. Pathfile: {root}\\{file}')
                    # skip to another file
                    continue
                d['bitrate'] = dict_inf['bitrate']
                d['video_codec'] = dict_inf['video']['codec']
                d['video_profile'] = dict_inf['video']['profile']
                d['video_resolution'] = dict_inf['video']['resolution']
                d['video_bitrate'] = dict_inf['video']['bitrate']
                # some videos dont have audio
                try:
                    d['audio_codec'] = dict_inf['audio']['codec']
                    d['audio_frequency'] = dict_inf['audio']['frequency']
                    d['audio_bitrate'] = dict_inf['audio']['bitrate']
                except:
                    d['audio_codec'] = ''
                    d['audio_frequency'] = ''
                    d['audio_bitrate'] = ''
                d['video_resolution_to_change'] = ''
                l.append(d)
    df = pd.DataFrame(l)
    
    return df


def get_video_details_with_group(df):

    df['key_join_checker'] = df['audio_codec'] + '-' + \
                             df['video_codec'] + '-' + \
                             df['video_resolution']

    # set group_encode
    df['group_encode'] = 1
    for index, row in df.iterrows():
        if index>0:
            group_encode_value_prev = df.loc[index-1, 'group_encode']
            if row['key_join_checker'] != df.loc[index-1, 'key_join_checker']:
                df.loc[index, 'group_encode'] = group_encode_value_prev + 1
            else:
                df.loc[index, 'group_encode'] = group_encode_value_prev
    
    return df
    
    
def get_list_chunk_videos_from_group(df, group_no, max_size_mb):

    max_size_bytes = max_size_mb * 1024**2
    mask = df['group_encode'].isin([group_no])
    
    df['file_path'] = df['file_folder'] + '\\' + \
                      df['file_name']
    
    df_group = df.loc[mask, :]
    list_chunk_videos = []
    chunk_size = 0
    list_videos = []
    for index, row in df_group.iterrows():
        if chunk_size + row['file_size'] > max_size_bytes:
            logging.info(f'join video from {len(list_videos)} files')
            list_chunk_videos.append(list_videos)
            
            list_videos = []
            chunk_size = 0
            
        list_videos.append(row['file_path'])
        chunk_size += row['file_size']
            
    if len(list_videos) > 0:
        logging.info(f'join video from {len(list_videos)} files')
        list_chunk_videos.append(list_videos)
        list_videos = []

    logging.info(f'group {group_no} will generate ' + \
                 f'{len(list_chunk_videos)} videos')
    return list_chunk_videos
        

def get_list_chunk_videos(df, max_size_mb):

    list_group = df['group_encode'].unique().tolist()
    list_final = []

    for group_no in list_group:
        group_no = str(group_no)
        list_chunk_videos = get_list_chunk_videos_from_group(df, group_no, 
                                                             max_size_mb)
        list_final += list_chunk_videos
        print('')
    
    return list_final
    
        
def join_videos(df, max_size_mb):

    path_folder_output = userpref_folderoutput()
    default_filename_output = input('Enter a default name for the joined ' +\
                                    'videos: ')      
    
    df['file_path'] = df['file_folder'] + '\\' + df['file_name']
    list_chunk_videos = get_list_chunk_videos(df, max_size_mb)
    df['file_output'] = ''
    for index, list_file_path in enumerate(list_chunk_videos):
        file_count = index+1
        file_name_output = f'{default_filename_output}-%03d.mp4' % file_count
        file_path_output = os.path.join(path_folder_output, file_name_output)
        join_mp4(list_file_path, file_path_output)
        
        # register file_output in dataframe
        mask_files_joined = df['file_path'].isin(list_file_path)
        df.loc[mask_files_joined, 'file_output'] = file_name_output
        
    print(f'total: {len(list_chunk_videos)} videos')
    
    return df


def make_reencode(df):

    df['file_folder_origin'] = df['file_folder']
    df['file_name_origin'] = df['file_name']
    df['file_size_origin'] = df['file_size']
    df['video_resolution_origin'] = df['video_resolution']
    mask_df_to_reencode = ~df['video_resolution_to_change'].isna()
    df_to_reencode = df.loc[mask_df_to_reencode, :]
    
    for index, row in df_to_reencode.iterrows():
        size_width, size_height = row['video_resolution_to_change'].split('x')
        
        path_file_origin = os.path.join(row['file_folder_origin'], 
                                        row['file_name_origin'])
        print(path_file_origin)
        path_folder_dest = r'videos_encoded'
        path_file_name_dest = str(index) + '.mp4'
        path_file_dest = os.path.join(path_folder_dest,
                                      path_file_name_dest)
        
        # todo reencode 
        # input path_folder_dest in column file_folder
        df.loc[index, 'file_folder'] = os.path.abspath(path_folder_dest)
        # input path_file_name_dest in column file_name
        df.loc[index, 'file_name'] = path_file_name_dest
        
        change_width_height_mp4(path_file_origin, size_height, 
                                size_width, path_file_dest)
        
        
        file_size = os.stat(path_file_dest).st_size
        df.loc[index, 'file_size'] = file_size
        df.loc[index, 'video_resolution'] = row['video_resolution_to_change']

        # from encoded video get video metadata
        metadata = get_video_details(path_file_dest)
        # register video metadata
        df.loc[index, 'bitrate'] = metadata['bitrate']
        df.loc[index, 'video_bitrate'] = dict_inf['video']['bitrate']
        df.loc[index, 'video_codec'] = metadata['video']['codec']
        df.loc[index, 'audio_codec'] = metadata['audio']['codec']
        df.loc[index, 'audio_bitrate']  = metadata['audio']['bitrate']
        df.loc[index, 'duration'] = metadata['duration']
        
    return df
        
        
def menu_ask():
    
    # ptbr
    # print('1-Gerar planilha listando os arquivos')
    # print('2-Processar reencode dos vídeos marcados na coluna '+
          # '"video_resolution_to_change"')
    # print('3-Agrupar vídeos em grupos de até 1 gb com mesmo codec e resolução')
    
    # eng
    print('1-Generate worksheet listing the files')
    print('2-Process reencode of videos marked in column ' +
          '"video_resolution_to_change"')
    print('3-Group videos into groups up to 1 gb with the same codec ' + \
          'and resolution')      
    
    # ptbr
    # msg_type_answer = 'Digite sua resposta: '
    
    # eng
    msg_type_answer = 'Type your answer: '
    make_report = int(input(f'\n{msg_type_answer}'))
    if make_report == 1:
        return 1
    elif make_report == 2:
        return 2
    elif make_report == 3:
        return 3
    else:
        # ptbr
        # msg_invalid_option = "Opção não disponível"
        
        # eng
        msg_invalid_option = "Invalid option"
        raise MyValidationError(msg_invalid_option)


def df_insert_row(row_number, df, row_value): 
    """
    A customized function to insert a row at any given position in the 
     dataframe.
    source: https://www.geeksforgeeks.org/insert-row-at-given-position-in-pandas-dataframe/
    :input: row_number: Int.
    :input: df: Dataframe.
    :input: row_value: Int.
    :return: Dataframe. df_result | 
             Boolean. False. If the row_number was invalid.
    """
    
    if row_number > df.index.max()+1: 
        print("df_insert_row: Invalid row_number") 
        return False
        
    # Slice the upper half of the dataframe 
    df1 = df[0:row_number] 
    
    # Store the result of lower half of the dataframe 
    df2 = df[row_number:] 
    
    # Inser the row in the upper half dataframe 
    df1.loc[row_number]=row_value 
    
    # Concat the two dataframes 
    df_result = pd.concat([df1, df2]) 
   
    # Reassign the index labels 
    df_result.index = [*range(df_result.shape[0])] 
   
    # Return the updated dataframe 
    return df_result 
            

def search_to_split_videos(df, mb_limit):

    def preprocess_df_split(df):
        
        df['file_path'] = df['file_folder'] + '\\' + df['file_name']
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
            dict_row_origin['file_folder']
        dict_row_dest['split_file_name_origin'] = dict_row_origin['file_name']
        dict_row_dest['split_file_size_origin'] = dict_row_origin['file_size']

        dict_row_dest['file_folder'] = path_folder_dest
        dict_row_dest['file_name'] = file_name_dest
        dict_row_dest['file_size'] = file_size_dest
        
        return dict_row_dest
        
    def get_row_number_from_filepath(df, file_path_origin):
    
        path_folder_origin = os.path.split(file_path_origin)[0]
        file_name_origin = os.path.split(file_path_origin)[1]
        
        mask1 = df['file_folder'].isin([path_folder_origin])
        mask2 = df['file_name'].isin([file_name_origin])
        mask_file = mask1 & mask2

        df_row_origin = df.loc[mask_file, :]
        dict_row_origin = df_row_origin.iloc[0]
        row_number = df_row_origin.index.values[0]
        
        return row_number
        
        
    def delete_fileorigin(df, file_path_origin):
    
        row_number = get_row_number_from_filepath(df, file_path_origin)
        df = df.drop(df.index[[row_number]])
        df = df.reset_index(drop=True)
        return df
        
      
    def update_df_files(df, file_path_origin, list_filepath_output):

        def include_rows_new_files(df, filepath_output):
            
            pathfile_output = os.path.abspath(filepath_output)
            # find the row_number of origin file
            row_number = get_row_number_from_filepath(df, file_path_origin)
            dict_row_origin = df.loc[row_number,:]                        
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

    def get_list_file_path_origin(df, size_limit):
    
        mask_to_be_split = df['file_size'] > size_limit
        df_to_be_split = df.loc[mask_to_be_split, :]
        list_file_path_origin = df_to_be_split.loc[:, 'file_path'].tolist()
        
        return list_file_path_origin 
   
    df = preprocess_df_split(df)
   
    recoil_sec = 10
    # TODO estimate the recoil_mbsize by video bitrate
    recoil_mbsize = 10
    size_limit = (mb_limit-recoil_mbsize) * 1024**2
    list_file_path_origin = get_list_file_path_origin(df=df, 
                                                      size_limit=size_limit)
    
    for file_path_origin in list_file_path_origin:
        output_folder_path = r'videos_splitted'
        list_filepath_output = split_mp4(largefile_path=file_path_origin, 
                                         recoil=recoil_sec, 
                                         mb_limit=mb_limit, 
                                         output_folder_path=output_folder_path)
                                         
        df = update_df_files(df=df, file_path_origin=file_path_origin, 
                             list_filepath_output=list_filepath_output)
                                
    return df 
    

def userpref_folderoutput():
    
    path_file = os.path.join('config', 'config.txt')
    variable_name = 'path_folder_output'
    variable_value = \
        handle_config_file(path_file, variable_name, 
                           set_value=None, parse=True)
    path_folder_output = variable_value['path_folder_output'][0]
    
    print(f'Use the folder path output as {path_folder_output}?')
    answer_use = input('(None for yes) Answer: ')
    if answer_use == '':
        pass
    else:
        path_folder_output = input('Inform the folder path output: ')
        handle_config_file(path_file, variable_name, 
                           set_value=path_folder_output, parse=False)
        
    return path_folder_output


def userpref_size_per_file_mb():
    
    path_file = os.path.join('config', 'config.txt')
    variable_name = 'size_per_file_mb'
    
    question = 'What should be the maximum size of each ' + \
               'file in mb (e.g.: 500)? '
    
    variable_value = \
        handle_config_file(path_file, variable_name, 
                           set_value=None, parse=True)
    value_got = variable_value[variable_name][0]
    
    print(f'The maximum size of each file will be {value_got}. Ok?')
    answer_use = input('(None for yes) Answer: ')
    if answer_use == '':
        pass
    else:
        value_got = input(question)
        handle_config_file(path_file, variable_name, 
                           set_value=value_got, parse=False)
        
    return value_got
    
    
def ensure_folder_existence(folders_name):
    
    for folder_name in folders_name:
        existence = os.path.isdir(f'./{folder_name}')
        if existence is False:
            os.mkdir(folder_name)

            
def ensure_folders_existence():

    folders_name = ['ts', 'videos_encoded', 'config', 'videos_splitted']
    ensure_folder_existence(folders_name)
  
  
def main():
    
    ensure_folders_existence()
    menu_answer = menu_ask()
    
    if menu_answer == 1:
        # create Dataframe of video details
        path_dir = input('\nPaste the folder link where are the video files: ')
        df = gen_report(path_dir)
        df.to_excel('video_details.xlsx', index=False)
        print(f'\nMake sure that the files in video_details.xlsx are in ' + \
              f'order \n and inform, if necessary, resolutions ' + \
              f'for reencode in the video_resolution_to_change column')
        break_point = input('Type Enter to continue')
        clean_cmd()
        main()
        return
        
    elif menu_answer == 2:
        # reencode videos mark in column video_resolution_to_change
        df = pd.read_excel(f'video_details.xlsx')
        df = make_reencode(df)
        df.to_excel('video_details.xlsx', index=False)  
        print('Reencode finished')
        break_point = input('Review the file and then type something to ' + \
                            'continue.')
                            
        df = pd.read_excel(f'video_details.xlsx')
        clean_cmd()
        main()
        return
    else:
        pass
        
    df = pd.read_excel(f'video_details.xlsx')
        
    # update video_details with groups
    df = get_video_details_with_group(df)
    df.to_excel('video_details.xlsx', index=False)
    print("File 'video_details.xlsx' was updated with " + \
          "group column to fast join\n")
    
    break_point = input('Review the file and then type something to start ' + \
                        'the process that look for videos that are too ' + \
                        'big and should be splitted')
    df = pd.read_excel('video_details.xlsx') 
    
    mb_limit = int(userpref_size_per_file_mb())
    
    # Find for file_video too big and split them
    df = search_to_split_videos(df, mb_limit=mb_limit)
    df.to_excel('video_details.xlsx', index=False)    

    break_point = input('Review the file and then type something to start '+
                        'the videos join process')
    
    # join all videos
    df = pd.read_excel(f'video_details.xlsx')                         
    df = join_videos(df, mb_limit)
    df.to_excel('video_details.xlsx', index=False)
    
    
if __name__ == "__main__":
    logging_config()
    main()
