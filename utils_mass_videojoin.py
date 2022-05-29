import glob
import hashlib
import os

import natsort
import pandas as pd
import unidecode


def get_file_name_dest(
    file_folder_origin, file_name_origin, prefix, file_extension=None
):
    """
    Create a hashed file name dest.
    Template: reencode_{file_name_origin}_{hash}.mp4"
    """

    file_folder_origin_encode = file_folder_origin.encode("utf-8")
    hash = hashlib.md5(file_folder_origin_encode).hexdigest()[:5]
    file_name_origin_without_extension = os.path.splitext(file_name_origin)[0]
    if file_extension == None:
        file_extension = os.path.splitext(file_name_origin)[1]
    else:
        file_extension = "." + file_extension

    file_name_dest = (
        prefix
        + file_name_origin_without_extension
        + "_"
        + hash
        + file_extension
    )
    return file_name_dest


def normalize_string(string_actual):
    """Replace letters with accent for letter without accent.
    Strip accents from characters.
    e.g.: Módulo Fácil -> Modulo Facil

    Args:
        string_actual (str): string to be normalized

    Returns:
        [str]: string normalized
    """

    string_norm = unidecode.unidecode(string_actual)

    return string_norm


def get_serie_sub_folder(serie_folder_path):
    def get_df_sub_folders(serie_folder_path):
        df = serie_folder_path.str.split("\\", expand=True)
        len_cols = len(df.columns)
        list_n_col_to_delete = []
        for n_col in range(len_cols - 1):
            serie = df.iloc[:, n_col]
            # check for column with more than 1 unique value (folder root)
            col_has_one_unique_value = check_col_unique_values(serie)
            if col_has_one_unique_value:
                name_col = df.columns[n_col]
                list_n_col_to_delete.append(name_col)

        df = df.drop(list_n_col_to_delete, axis=1)
        return df

    df_sub_folders = get_df_sub_folders(serie_folder_path)
    serie_first_column = df_sub_folders.iloc[:, 0]
    return serie_first_column


def check_col_unique_values(serie):

    serie_unique = serie.drop_duplicates(keep="first")
    list_unique_values = serie_unique.unique().tolist()
    qt_unique_values = len(list_unique_values)
    if qt_unique_values == 1:
        return True
    else:
        return False


def sort_human(list_):

    list_ = natsort.natsorted(list_)
    return list_


def sort_df_column_from_list(df, column_name, sorter):
    """
    :input: df: DataFrame
    :input: column_name: String
    :input: sorter: List
    :return: DataFrame
    """

    sorterIndex = dict(zip(sorter, range(len(sorter))))
    df["order"] = df[column_name].map(sorterIndex)
    df.sort_values(["order"], ascending=[True], inplace=True)
    df.drop(columns=["order"], axis=1, inplace=True)
    return df


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

    if row_number > df.index.max() + 1:
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
        hr, min, sec = map(float, str_hh_mm_ss_ms.split(":"))
        return True
    except:
        raise Exception(
            f'The time value "{str_hh_mm_ss_ms} "'
            + "need to be in format: hh:mm:ss.ms"
        )


def exclude_all_files_from_folder(path_folder):

    path_folder_regex = os.path.join(path_folder, "*")
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
