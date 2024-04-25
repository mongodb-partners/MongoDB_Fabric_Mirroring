import os
from dotenv import load_dotenv
from init_sync import init_sync
from listening import listening

def mirror(
    mongodb_conn_str: str,
    mongodb_db_name: str,
    mongodb_coll_name: str,
    lz_url: str,
    lz_app_id: str = None,
    lz_secret: str = None,
    lz_tenant_id: str = None,
):
    load_dotenv()
    if not lz_app_id:
        lz_app_id = os.getenv("APP_ID")
    if not lz_secret:
        lz_secret = os.getenv("SECRET")
    if not lz_tenant_id:
        lz_tenant_id = os.getenv("TENANT_ID")
    if (
        not mongodb_conn_str
        or not mongodb_db_name
        or not mongodb_coll_name
        or not lz_url
        or not lz_app_id
        or not lz_secret
        or not lz_tenant_id
    ):
        raise ValueError("Invalid parameter value detected!")

    mongodb_params = {
        "conn_str": mongodb_conn_str,
        "db_name": mongodb_db_name,
        "collection": mongodb_coll_name,
    }
    
    lz_params = {
        "url": lz_url,
        "app_id": lz_app_id,
        "secret": lz_secret,
        "tenant_id": lz_tenant_id,
    }

    init_sync(mongodb_params, lz_params)
    
    # gap?
    
    # new thread?:
    listening(mongodb_params, lz_params)
