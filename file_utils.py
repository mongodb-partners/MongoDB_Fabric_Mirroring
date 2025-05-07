import os
import pickle
from enum import Enum
from typing import Any
import utils
from push_file_to_lz import push_file_to_lz, get_file_from_lz, delete_file_from_lz


class FileType(Enum):
    PICKLE = "pickle"
    TEXT = "text"


FILETYPE_TO_READ_MODE_MAP = {
    FileType.PICKLE: "rb",
    FileType.TEXT: "r",
}

FILETYPE_TO_WRITE_MODE_MAP = {
    FileType.PICKLE: "wb",
    FileType.TEXT: "w",
}


# def read_from_file(table_name: str, file_name: str, file_type: FileType):
#     table_path = utils.get_table_dir(table_name)
#     file_full_path = os.path.join(table_path, file_name)
#     print("File path ; ", file_full_path)
#     # always read from LZ first
#     if get_file_from_lz(table_name, file_name) : # and os.path.exists(file_full_path):
#         with open(
#             file_full_path, FILETYPE_TO_READ_MODE_MAP.get(file_type, "r")
#         ) as file:
#             if file_type == FileType.PICKLE:
#                 obj = pickle.load(file)
#             elif file_type == FileType.TEXT:
#                 obj = file.read()
#             else:
#                 obj = None
#             return obj
#     else:
#         return None


def read_from_file(table_name: str, file_name: str, file_type: FileType):
    table_path = utils.get_table_dir(table_name)
    file_full_path = os.path.join(table_path, file_name)
    # always read from LZ first
    response_status_code, file_content = get_file_from_lz(table_name, file_name)
    if response_status_code == 200: 
        if file_type == FileType.PICKLE:
            obj = pickle.loads(file_content.content)
            print("Type of object: ", isinstance(obj, bytes))
            # Check if the result is itself a pickled object (nested)
            if isinstance(obj, bytes):
                obj = pickle.loads(obj)
            print("Unpickled object: ", obj)
            return obj

        elif file_type == FileType.TEXT:
            return file_content.content.decode('utf-8')
        else:
            return None
    else:
        return None

def write_to_file(obj: Any, table_name: str, file_name: str, file_type: FileType):
    table_path = utils.get_table_dir(table_name)
    file_full_path = os.path.join(table_path, file_name)
    with open(file_full_path, FILETYPE_TO_WRITE_MODE_MAP.get(file_type, "w")) as file:
        if file_type == FileType.PICKLE:
            pickle.dump(obj, file)
        elif file_type == FileType.TEXT:
            file.write(obj)
    # write to LZ
    push_file_to_lz(file_full_path, table_name)


def append_to_file(obj: Any, table_name: str, file_name: str, file_type: FileType):
    table_path = utils.get_table_dir(table_name)
    file_full_path = os.path.join(table_path, file_name)
    append_mode = "ab" if file_type == FileType.PICKLE else "a"
    
    # Create file if it doesn't exist
    if not os.path.exists(file_full_path):
        with open(file_full_path, FILETYPE_TO_WRITE_MODE_MAP.get(file_type, "w")) as f:
            f.write(f"\n{'Column Name':<20} | {'Original Value':<20} | {'Converting Value':<20}\n{'-'*70}\n")
    
    with open(file_full_path, append_mode) as file:
        if file_type == FileType.PICKLE:
            pickle.dump(obj, file)
        elif file_type == FileType.TEXT:
            file.write(obj)
    # write to LZ
    # push_file_to_lz(file_full_path, table_name)


def delete_file(table_name: str, file_name: str):
    file_full_path = os.path.join(utils.get_table_dir(table_name), file_name)
    os.remove(file_full_path)
    # delete from LZ
    delete_file_from_lz(table_name, file_name)
