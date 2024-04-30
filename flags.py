import logging

logger = logging.getLogger(__name__)

init_flags = []

def get_init_flag(collection: str):
    logger.debug(f"Getting init flag for collection {collection}. Flag={collection in init_flags}")
    return collection in init_flags

def set_init_flag(collection: str):
    logger.debug(f"Setting init flag for collection {collection}")
    init_flags.append(collection)
    
def clear_init_flag(collection: str):
    logger.debug(f"Clearing init flag for collection {collection}")
    init_flags.remove(collection)