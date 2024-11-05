import os
from constants import DATA_FILES_PATH, FILE_NAME_LENGTH


def to_string(obj) -> str:
    return str(obj)

def get_table_dir(table_name: str) -> str:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    table_dir = os.path.join(current_dir, DATA_FILES_PATH, table_name + os.sep)
    os.makedirs(table_dir, exist_ok=True)
    return table_dir
#changes to get next parquet file num based on the last parquet from LZ, it will pass 0 if first file
def get_parquet_full_path_filename(table_name: str, parquet_filename_int_list: int, prefix: str = "") -> str:
    table_dir = get_table_dir(table_name)
    # parquet_filename_int_list = [
    #     int(os.path.splitext(filename)[0].removeprefix(prefix))
    #     for filename in os.listdir(table_dir)
    #     if os.path.splitext(filename)[1] == ".parquet"
    #     and os.path.splitext(filename)[0].removeprefix(prefix).isnumeric()
    # ]
    #if parquet_filename_int_list:
    #    return os.path.join(table_dir, prefix + __num_to_filename(max(parquet_filename_int_list) + 1))
    return os.path.join(table_dir, prefix + __num_to_filename(parquet_filename_int_list + 1))
    # else:
    #     return os.path.join(table_dir, prefix + __num_to_filename(1))
    
#as temp files will be stored in local only, kept the original logic a is
def get_temp_parquet_full_path_filename(table_name: str, prefix: str = "") -> str:
    table_dir = get_table_dir(table_name)
    tmp_parquet_filename_int_list = [
        int(os.path.splitext(filename)[0].removeprefix(prefix))
        for filename in os.listdir(table_dir)
        if os.path.splitext(filename)[1] == ".parquet"
        and os.path.splitext(filename)[0].removeprefix(prefix).isnumeric()
    ]
    if tmp_parquet_filename_int_list:
        return os.path.join(table_dir, prefix + __num_to_filename(max(tmp_parquet_filename_int_list) + 1))
    else:
        return os.path.join(table_dir, prefix + __num_to_filename(1))


def __num_to_filename(num: int) -> str:
    int_to_str = str(num)
    leading_zeros = FILE_NAME_LENGTH - len(int_to_str)
    return "0" * leading_zeros + int_to_str + ".parquet"
