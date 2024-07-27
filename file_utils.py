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


def read_from_file(table_name: str, file_name: str, file_type: FileType):
    table_path = utils.get_table_dir(table_name)
    file_full_path = os.path.join(table_path, file_name)
    # always read from LZ first
    if get_file_from_lz(table_name, file_name) and os.path.exists(file_full_path):
        with open(
            file_full_path, FILETYPE_TO_READ_MODE_MAP.get(file_type, "r")
        ) as file:
            if file_type == FileType.PICKLE:
                obj = pickle.load(file)
            elif file_type == FileType.TEXT:
                obj = file.read()
            else:
                obj = None
            return obj
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


def delete_file(table_name: str, file_name: str):
    file_full_path = os.path.join(utils.get_table_dir(table_name), file_name)
    os.remove(file_full_path)
    # delete from LZ
    delete_file_from_lz(table_name, file_name)
