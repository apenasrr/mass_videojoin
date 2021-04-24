import os
import glob
import pandas as pd


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
    df1.loc[row_number] = row_value

    # Concat the two dataframes
    df_result = pd.concat([df1, df2])

    # Reassign the index labels
    df_result.index = [*range(df_result.shape[0])]

    # Return the updated dataframe
    return df_result


def time_is_hh_mm_ss_ms(str_hh_mm_ss_ms):
    """test if time value is format hh:mm:ss.ms

    Args:
        str_hh_mm_ss_ms (str): time value

    Raises:
        Exception: incorrrect format

    Returns:
        bol: True if valid
    """

    try:
        hr, min, sec = map(float, str_hh_mm_ss_ms.split(':'))
        return True
    except:
        raise Exception(f'The time value "{str_hh_mm_ss_ms} "' +
                        'need to be in format: hh:mm:ss.ms')


def exclude_all_files_from_folder(path_folder):

    path_folder_regex = os.path.join(path_folder, '*')
    r = glob.glob(path_folder_regex)
    for i in r:
        os.remove(i)


def create_report_backup(df, path_file_report, tag):

    path_folder = os.path.dirname(path_file_report)
    file_name = os.path.basename(path_file_report)
    file_name_without_extension = os.path.splitext(file_name)[0]
    file_name_backup = file_name_without_extension + "_" + tag + ".xlsx"
    path_file_backup = os.path.join(path_folder, file_name_backup)
    df.to_excel(path_file_backup, index=False)


def get_folder_script_path():

    folder_script_path_relative = os.path.dirname(__file__)
    folder_script_path = os.path.realpath(folder_script_path_relative)
    return folder_script_path