import os
from constants import DATA_FILES_PATH, FILE_NAME_LENGTH


def to_string(obj) -> str:
    return str(obj)

def get_parquet_filename(table_name: str) -> str:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    table_dir = os.path.join(current_dir, DATA_FILES_PATH, table_name + os.sep)
    if not os.path.exists(table_dir):
        os.makedirs(table_dir, exist_ok=True)
        return os.path.join(table_dir, __num_to_filename(1))
    else:
        parquet_filename_int_list = [
            int(os.path.splitext(filename)[0])
            for filename in os.listdir(table_dir)
            if os.path.splitext(filename)[1] == ".parquet"
            and os.path.splitext(filename)[0].isnumeric()
        ]
        if parquet_filename_int_list:
            return os.path.join(table_dir, __num_to_filename(max(parquet_filename_int_list) + 1))
        else:
            return os.path.join(table_dir, __num_to_filename(1))


def __num_to_filename(num: int) -> str:
    int_to_str = str(num)
    leading_zeros = FILE_NAME_LENGTH - len(int_to_str)
    return "0" * leading_zeros + int_to_str + ".parquet"
