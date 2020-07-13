"""
    Create by: apenasrr
    Source: https://github.com/apenasrr/mass_videojoin
"""

import ast   


def config_file_parser_values(list_found, variable_name):
    
    list_found_parsed = []
    dict_build = {}
    dict_values = {}
    for item in list_found:
        item_parsed = ast.literal_eval(item)
        if isinstance(item_parsed, dict):
            dict_values.update(item_parsed)
        else:
            list_found_parsed.append(item_parsed)

    if len(dict_values)!=0:
        dict_build[variable_name] = dict_values
        if len(list_found_parsed) != 0:
            dict_build[variable_name]['others'] = list_found_parsed
    else:
        dict_build[variable_name] = list_found_parsed
    return dict_build
    

def handle_config_file(path_file, variable_name, set_value=None, 
                       parse=False):

    def get_updated_line(variable_name, set_value):
        if isinstance(set_value, dict):
            set_value_parsed = set_value
        else:
            set_value_parsed = set_value
            
        if isinstance(set_value_parsed, dict):
            updated_line = f"{variable_name}={set_value_parsed}\n"
        else:
            updated_line = f"{variable_name}='{set_value_parsed}'\n"
        return updated_line
    
    def get_str_value(line):
        line_components = line.split('=')
        str_value = line_components[1]
        str_value = str_value.replace("\n", '')
        return str_value

    def get_item_parsed(line):
        str_value = get_str_value(line)
        item_parsed = ast.literal_eval(str_value)
        return item_parsed
        
    def value_is_dict(item_parsed):
        is_dict = isinstance(item_parsed, dict)
        return is_dict

    def is_same_key(item_parsed, set_value):
        key_item_parsed = next(iter(item_parsed))
        key_set_value = next(iter(set_value))
        same_key = key_item_parsed == key_set_value
        return same_key
        
    config_file = open(path_file, 'r+')
    content_lines = []

    list_found = []
    dont_found = True
    if set_value:
        updated_line = get_updated_line(variable_name, set_value)
        for line in config_file:
            if f'{variable_name}=' in line:
                item_parsed = get_item_parsed(line)
                if value_is_dict(item_parsed):
                    if is_same_key(item_parsed, set_value):
                        dont_found = False
                        content_lines.append(updated_line)
                    else:
                        content_lines.append(line)
                else:
                    dont_found = False
                    content_lines.append(updated_line)
            else:
                content_lines.append(line)
              
        if dont_found:
            
            # include variable_name and value at botton of file
            content_lines.append(updated_line)

        # save and finish file
        config_file.seek(0)
        config_file.truncate()
        config_file.writelines(content_lines)
        config_file.close()
        
    else:
        for line in config_file:
            if f'{variable_name}=' in line:
                str_value = get_str_value(line)
                list_found.append(str_value)
        # finish file and return value
        config_file.close()
        if parse:
            dict_build = config_file_parser_values(list_found, variable_name)
            return dict_build
        else:
            dict_build = {}
            dict_build[variable_name] = list_found 
            return dict_build
                