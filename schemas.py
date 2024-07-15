import logging
import os
import threading
import pickle

from utils import get_table_dir
from constants import INTERNAL_SCHEMA_FILE_NAME, COLUMN_RENAMING_FILE_NAME

logger = logging.getLogger(__name__)

__schemas = {}
__locks = {}
__column_renamings = {}


def write_table_schema_to_file(table_name: str):
    schema_of_this_table = __schemas.get(table_name, None)
    if not schema_of_this_table:
        return
    table_dir = get_table_dir(table_name)
    schema_file_path = os.path.join(table_dir, INTERNAL_SCHEMA_FILE_NAME)
    with open(schema_file_path, "wb") as schema_file:
        logger.info(f"writing schema of {table_name} into file")
        pickle.dump(schema_of_this_table, schema_file)

def init_table_schema(table_name: str, table_schema: dict):
    __schemas[table_name] = table_schema
    __locks[table_name] = threading.Lock()    

def get_table_schema(table_name: str) -> dict:
    return __schemas.get(table_name, None)

def get_table_column_schema(table_name: str, column_name: str) -> dict:
    return __schemas.get(table_name, {}).get(column_name, None)

def append_schema_column(table_name: str, column_name: str, column_schema: dict):
    # 0. get the lock of the collection
    with __locks[table_name]:
        # 1. append column
        __schemas[table_name][column_name] = column_schema
        # 2. write into file
        write_table_schema_to_file(table_name)


def write_table_column_renaming_to_file(table_name: str):
    table_column_renaming = __column_renamings.get(table_name, None)
    if not table_column_renaming:
        return
    table_dir = get_table_dir(table_name)
    column_renaming_file_path = os.path.join(table_dir, COLUMN_RENAMING_FILE_NAME)
    with open(column_renaming_file_path, "wb") as column_renaming_file:
        logger.info(f"writing column renaming of {table_name} into file")
        pickle.dump(table_column_renaming, column_renaming_file)

def init_column_renaming(table_name: str, column_renaming: dict):
    __column_renamings[table_name] = column_renaming

def add_column_renaming(table_name: str, original_column_name: str, new_column_name: str):
    # TODO: determine if we need lock here
    __column_renamings[table_name][original_column_name] = new_column_name
    write_table_column_renaming_to_file(table_name)
    
def find_column_renaming(table_name: str, column_name: str) -> str:
    return __column_renamings.get(table_name, {}).get(column_name, None)

def get_table_column_renaming(table_name: str) -> dict:
    return __column_renamings.get(table_name, None)
